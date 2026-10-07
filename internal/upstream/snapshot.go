package upstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/format/rtpmjpeg"
	"github.com/pion/rtp"
)

// CleanJPEG sanitizes a decoded JPEG frame. Some IP cameras (e.g. TP-Link Tapo)
// embed a complete JPEG image (including SOI, DQT, DHT, SOF0, SOS) within the RTP payload,
// which causes RFC 2435 RTP/MJPEG decoders to produce a frame with two concatenated headers.
// This function strips any duplicate prefix so the resulting JPEG decodes cleanly in all browsers.
func CleanJPEG(data []byte) []byte {
	if len(data) < 4 || data[0] != 0xFF || data[1] != 0xD8 {
		return data
	}
	searchLimit := len(data) - 1
	if searchLimit > 2048 {
		searchLimit = 2048
	}
	for i := 2; i < searchLimit; i++ {
		if data[i] == 0xFF && data[i+1] == 0xD8 {
			return data[i:]
		}
	}
	return data
}

// FetchSingleSnapshot retrieves a single JPEG frame from an HTTP or RTSP source.
func FetchSingleSnapshot(ctx context.Context, rawURL string) ([]byte, error) {
	var frame []byte
	var err error
	if strings.HasPrefix(rawURL, "rtsp://") || strings.HasPrefix(rawURL, "rtsps://") {
		frame, err = CaptureRTSPSnapshot(ctx, rawURL)
	} else {
		frame, err = fetchHTTPSnapshot(ctx, rawURL)
	}
	if err != nil {
		return nil, err
	}
	return CleanJPEG(frame), nil
}

// fetchHTTPSnapshot fetches a single JPEG from an HTTP/HTTPS endpoint.
func fetchHTTPSnapshot(ctx context.Context, httpURL string) ([]byte, error) {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, httpURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "video-amplifier/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("snapshot upstream returned HTTP %d", resp.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxJPEGFrameSize))
	if err != nil {
		return nil, err
	}

	if len(data) < 4 || data[0] != 0xFF || data[1] != 0xD8 {
		return nil, errors.New("invalid JPEG image received from snapshot endpoint")
	}

	return data, nil
}

// CaptureRTSPSnapshot connects to an RTSP stream, extracts the first JPEG frame,
// and falls back to external decoding (VLC/ffmpeg) if only H.264 is present.
func CaptureRTSPSnapshot(ctx context.Context, rtspURL string) ([]byte, error) {
	parsedURL, err := base.ParseURL(rtspURL)
	if err != nil {
		return nil, fmt.Errorf("invalid RTSP URL: %w", err)
	}

	client := &gortsplib.Client{
		Scheme:       parsedURL.Scheme,
		Host:         parsedURL.Host,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	if err := client.Start(); err != nil {
		// If direct RTSP connection fails, try external fallback
		return captureExternal(ctx, rtspURL)
	}
	defer client.Close()

	desc, _, err := client.Describe(parsedURL)
	if err != nil {
		return captureExternal(ctx, rtspURL)
	}

	// 1. Check for MJPEG format
	var formaMJPEG *format.MJPEG
	mediMJPEG := desc.FindFormat(&formaMJPEG)
	if mediMJPEG != nil && formaMJPEG != nil {
		dec := &rtpmjpeg.Decoder{}
		if err := dec.Init(); err == nil {
			if _, err := client.Setup(desc.BaseURL, mediMJPEG, 0, 0); err == nil {
				frameChan := make(chan []byte, 1)
				client.OnPacketRTP(mediMJPEG, formaMJPEG, func(pkt *rtp.Packet) {
					frame, err := dec.Decode(pkt)
					if err == nil && len(frame) > 0 {
						select {
						case frameChan <- frame:
						default:
						}
					}
				})

				if _, err := client.Play(nil); err == nil {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(4 * time.Second):
					case frame := <-frameChan:
						if len(frame) >= 4 && frame[0] == 0xFF && frame[1] == 0xD8 {
							return frame, nil
						}
					}
				}
			}
		}
	}

	// 2. Stream is H.264 or H.265: close gortsplib and delegate to external capture
	client.Close()
	return captureExternal(ctx, rtspURL)
}

func findExecutable(names ...string) (string, error) {
	for _, name := range names {
		if path, err := exec.LookPath(name); err == nil {
			return path, nil
		}
		if fi, err := os.Stat(name); err == nil && !fi.IsDir() && fi.Mode()&0111 != 0 {
			return name, nil
		}
	}
	return "", fmt.Errorf("none of %v found", names)
}

func captureExternal(ctx context.Context, rtspURL string) ([]byte, error) {
	var errs []string

	// Check ffmpeg first
	if ffmpegPath, err := findExecutable("ffmpeg", "/usr/bin/ffmpeg", "/usr/local/bin/ffmpeg", "/opt/homebrew/bin/ffmpeg"); err == nil {
		cmd := exec.CommandContext(ctx, ffmpegPath,
			"-hide_banner",
			"-loglevel", "error",
			"-rtsp_transport", "tcp",
			"-i", rtspURL,
			"-frames:v", "1",
			"-f", "image2",
			"-",
		)
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err == nil && stdout.Len() > 0 {
			return stdout.Bytes(), nil
		} else if err != nil {
			errs = append(errs, fmt.Sprintf("ffmpeg: %v (%s)", err, strings.TrimSpace(stderr.String())))
		}
	}

	// Check VLC
	if vlcPath, err := findExecutable("vlc", "/opt/homebrew/bin/vlc", "/Applications/VLC.app/Contents/MacOS/VLC", "/usr/local/bin/vlc", "/usr/bin/vlc"); err == nil {
		tmpDir, err := os.MkdirTemp("", "vidamp-snap-*")
		if err == nil {
			defer os.RemoveAll(tmpDir)

			expectedFile := filepath.Join(tmpDir, "snap.jpeg")
			cmd := exec.CommandContext(ctx, vlcPath,
				"-I", "dummy",
				rtspURL,
				"--vout=dummy",
				"--video-filter=scene",
				"--scene-format=jpeg",
				"--scene-ratio=1",
				"--scene-replace",
				"--scene-prefix=snap",
				"--scene-path="+tmpDir,
				"--run-time=2",
				"vlc://quit",
			)
			_ = cmd.Run()

			// Wait up to 1 second for scene filter to write file
			for i := 0; i < 10; i++ {
				if data, err := os.ReadFile(expectedFile); err == nil && len(data) > 0 {
					return data, nil
				}
				time.Sleep(100 * time.Millisecond)
			}
			errs = append(errs, "vlc: snapshot file not produced")
		}
	}

	return nil, fmt.Errorf("no external video decoder available to extract snapshot: %s", strings.Join(errs, "; "))
}

// StartSnapshotIngest starts a background loop ingesting MJPEG from an RTSP snapshot URL.
func StartSnapshotIngest(ctx context.Context, cs *CameraStream, snapURL string) {
	if snapURL == "" || snapURL == cs.Config.UpstreamURL {
		return
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			err := runSnapshotIngest(ctx, cs, snapURL)
			if err != nil && !errors.Is(err, context.Canceled) && ctx.Err() == nil {
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
				}
			}
		}
	}()
}

func runSnapshotIngest(ctx context.Context, cs *CameraStream, snapURL string) error {
	parsedURL, err := base.ParseURL(snapURL)
	if err != nil {
		return err
	}

	client := &gortsplib.Client{
		Scheme:       parsedURL.Scheme,
		Host:         parsedURL.Host,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	if err := client.Start(); err != nil {
		return err
	}
	defer client.Close()

	desc, _, err := client.Describe(parsedURL)
	if err != nil {
		return err
	}

	var formaMJPEG *format.MJPEG
	mediMJPEG := desc.FindFormat(&formaMJPEG)
	if mediMJPEG == nil || formaMJPEG == nil {
		return errors.New("no MJPEG format in snapshot stream")
	}

	dec := &rtpmjpeg.Decoder{}
	if err := dec.Init(); err != nil {
		return err
	}

	if _, err := client.Setup(desc.BaseURL, mediMJPEG, 0, 0); err != nil {
		return err
	}

	client.OnPacketRTP(mediMJPEG, formaMJPEG, func(pkt *rtp.Packet) {
		frame, err := dec.Decode(pkt)
		if err == nil && len(frame) > 0 {
			cs.BroadcastMJPEGFrame(frame)
		}
	})

	if _, err := client.Play(nil); err != nil {
		return err
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- client.Wait()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		return err
	}
}
