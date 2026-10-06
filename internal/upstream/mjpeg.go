package upstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

const (
	maxJPEGFrameSize = 8 * 1024 * 1024 // 8MB safety ceiling for high-res snapshot/frame
	readChunkSize    = 32 * 1024       // 32KB read buffer
)

var (
	jpegSOI = []byte{0xFF, 0xD8} // Start of Image
	jpegEOI = []byte{0xFF, 0xD9} // End of Image
)

// MJPEGDriver ingests an HTTP multipart MJPEG stream.
type MJPEGDriver struct {
	stream     *CameraStream
	httpClient *http.Client
	respBody   io.ReadCloser
}

// NewMJPEGDriver initializes a driver for HTTP MJPEG streams.
func NewMJPEGDriver(stream *CameraStream) *MJPEGDriver {
	client := &http.Client{
		Timeout: 0, // Streaming connection has no total timeout
	}
	return &MJPEGDriver{
		stream:     stream,
		httpClient: client,
	}
}

// Start begins ingesting the upstream MJPEG feed.
func (d *MJPEGDriver) Start(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.stream.Config.UpstreamURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create http request: %w", err)
	}

	req.Header.Set("User-Agent", "video-amplifier/1.0")
	req.Header.Set("Accept", "multipart/x-mixed-replace, image/jpeg, */*")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("upstream http request failed: %w", err)
	}
	defer resp.Body.Close()
	d.respBody = resp.Body

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("upstream responded with HTTP %d %s", resp.StatusCode, resp.Status)
	}

	d.stream.SetState(StateStreaming)
	d.stream.ReportSuccess()
	d.stream.logger.Info("Connected to upstream MJPEG camera feed")

	return d.readFrames(ctx, resp.Body)
}

// Stop closes the active HTTP response body.
func (d *MJPEGDriver) Stop() {
	if d.respBody != nil {
		_ = d.respBody.Close()
		d.respBody = nil
	}
}

// readFrames streams data from upstream and scans for JPEG delimiters (SOI 0xFFD8, EOI 0xFFD9).
func (d *MJPEGDriver) readFrames(ctx context.Context, r io.Reader) error {
	buf := make([]byte, readChunkSize)
	var frameBuf bytes.Buffer
	inFrame := false

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := r.Read(buf)
		if n > 0 {
			d.stream.AddBytesReceived(int64(n))
			chunk := buf[:n]

			for len(chunk) > 0 {
				if !inFrame {
					// Search for SOI (0xFF, 0xD8)
					idx := bytes.Index(chunk, jpegSOI)
					if idx == -1 {
						// Discard chunk without SOI
						break
					}
					// Found start of frame
					inFrame = true
					frameBuf.Reset()
					chunk = chunk[idx:]
				}

				// Search for EOI (0xFF, 0xD9)
				eoiIdx := bytes.Index(chunk, jpegEOI)
				if eoiIdx == -1 {
					// Entire chunk belongs to current frame
					frameBuf.Write(chunk)
					if frameBuf.Len() > maxJPEGFrameSize {
						d.stream.logger.Warn("JPEG frame exceeded maximum size limit, resetting frame scanner",
							slog.Int("size", frameBuf.Len()),
						)
						inFrame = false
						frameBuf.Reset()
					}
					break
				}

				// Found EOI
				frameBuf.Write(chunk[:eoiIdx+2])
				chunk = chunk[eoiIdx+2:]
				inFrame = false

				// Frame complete, broadcast to subscribers
				completedFrame := make([]byte, frameBuf.Len())
				copy(completedFrame, frameBuf.Bytes())
				d.stream.BroadcastMJPEGFrame(completedFrame)
				frameBuf.Reset()
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				return err
			}
			return fmt.Errorf("read error from upstream MJPEG: %w", err)
		}
	}
}

// FetchSingleSnapshot issues a one-off HTTP request to fetch a single JPEG snapshot.
func FetchSingleSnapshot(ctx context.Context, snapshotURL string) ([]byte, error) {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, snapshotURL, nil)
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

	// Verify JPEG header
	if len(data) < 4 || data[0] != 0xFF || data[1] != 0xD8 {
		return nil, errors.New("invalid JPEG image received from snapshot endpoint")
	}

	return data, nil
}
