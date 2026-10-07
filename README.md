# video-amplifier

[![Go Version](https://img.shields.io/badge/Go-1.23%2B-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Zero CGO](https://img.shields.io/badge/CGO-zero-brightgreen)](#tech-stack--architecture)
[![Prometheus](https://img.shields.io/badge/metrics-Prometheus-E6522C?logo=prometheus)](/metrics)

A high-performance, production-grade IP camera streaming proxy designed by Staff Software and Principal Site Reliability Engineers.

`video-amplifier` acts as a **single-tenant upstream consumer** per camera feed, fanning out audio/video streams to hundreds of concurrent downstream clients (NVRs, Home Assistant, dashboards, web portals) while strictly isolating fragile camera hardware from connection spikes and slow consumer backpressure.

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph Upstream["Upstream IP Cameras (Fragile Hardware)"]
        Cam1["Front Door 4K<br/>(RTSP / H.264 / H.265)"]
        Cam2["Workshop Overhead<br/>(HTTP / MJPEG)"]
    end

    subgraph VA["video-amplifier (Single Ingest Tenant)"]
        CB["Circuit Breaker & Backoff Engine"]
        IngestRTSP["RTSP Ingest Worker<br/>(TCP/UDP)"]
        IngestMJPEG["MJPEG Ingest Worker<br/>(multipart stream)"]
        SnapCache["In-Memory Snapshot Cache<br/>(/snapshot.jpg)"]

        subgraph Fanout["Downstream Backpressure Isolation"]
            RB1["Bounded Ring Buffer<br/>(Client A - Fast)"]
            RB2["Bounded Ring Buffer<br/>(Client B - Slow Cellular)"]
            RTSPRelay["RTSP Server Stream<br/>(gortsplib/v5)"]
        end
    end

    subgraph Downstream["Downstream Consumers (Hundreds)"]
        HA["Home Assistant"]
        NVR["Frigate / Blue Iris"]
        WebPortal["Web Browser (MJPEG)"]
        Mobile["Mobile Dashboard"]
    end

    Cam1 -->|Single Connection| IngestRTSP
    Cam2 -->|On-Demand Ingest| IngestMJPEG
    IngestRTSP --> CB
    IngestMJPEG --> CB

    IngestMJPEG --> SnapCache
    IngestMJPEG --> RB1
    IngestMJPEG --> RB2
    IngestRTSP --> RTSPRelay

    RB1 --> WebPortal
    RB2 -.->|Drop Oldest Frames| Mobile
    RTSPRelay --> NVR
    RTSPRelay --> HA
    SnapCache --> WebPortal
```

---

## Core Capabilities

### 1. Zero-Transcode Pure Passthrough
- **Zero CPU Encoding Overhead**: Streams are multiplexed as pure RTP/H.264/H.265 packets or JPEG frames without transcoding.
- **Microsecond Latency**: Packets are routed immediately through bounded ring buffers.

### 2. Upstream Resilience & Lifecycle Management
- **Connection Strategies**:
  - `always-on`: 24/7 persistent ingestion with health checks, ideal for critical security NVR recordings.
  - `on-demand`: Ingests from the camera only when ≥1 downstream viewer is active. Automatically starts an idle grace period (e.g. 30s) when the last viewer leaves. If a new viewer connects during the grace period, the timer is aborted with zero disruption.
- **Self-Healing Reconnect Engine**: Exponential backoff with randomized jitter (±20%) prevents thundering herd connection storms following network partitions or camera reboots.
- **Circuit Breaking**: Isolates persistently failing upstream cameras after repeated timeouts or 401 Unauthorized errors, protecting local subnets from connection storms.

### 3. Downstream Multiplexing & Zero Head-of-Line Blocking
- **Isolated Per-Client Ring Buffers**: Each downstream consumer receives an independent bounded queue. Slow consumers on poor wireless/cellular links can never stall the upstream feed or impact other viewers.
- **Backpressure Handling**:
  - **MJPEG**: Automatically drops older frames at queue capacity to keep the client receiving the freshest real-time frames, while incrementing `video_amplifier_downstream_dropped_frames_total`.
  - **RTSP**: Tracks reader session write queues and latency. If a client stalls beyond `client_timeout`, the connection is torn down cleanly.

### 4. SRE-First Observability
- **Prometheus Metrics (`:9090/metrics`)**:
  - `video_amplifier_upstream_connected{camera="name"}`
  - `video_amplifier_upstream_reconnects_total{camera="name"}`
  - `video_amplifier_upstream_bytes_received_total{camera="name"}`
  - `video_amplifier_downstream_active_clients{camera="name", protocol="mjpeg|rtsp"}`
  - `video_amplifier_downstream_dropped_frames_total{camera="name", client_id="xyz"}`
  - Full Go runtime collectors (`go_goroutines`, `go_memstats_*`, process CPU & memory).
- **Automated Credential Redaction**: All RTSP/HTTP camera credentials (`user:password@host`) are automatically redacted in structured `log/slog` logs (e.g. `rtsp://admin:*****@192.168.1.50/live`).
- **Health Probes**:
  - `/healthz`: Liveness check (process operational).
  - `/readyz`: Readiness check (all stream routes active and upstream engine responsive).
### 5. Self-Contained Web Interface & Live Dashboard
- **Responsive Multi-Stream Grid**: Directly monitor all camera streams at `http://localhost:8080/` (or `/ui` / `/dashboard`) via a modern dark-mode layout.
- **Air-Gapped Operation**: 100% self-contained with embedded HTML/CSS/JS (zero external CDNs or external asset fetches), ensuring full functionality in isolated security subnets.
- **Stream URL Chips & One-Click Copy**: Instantly copy downstream RTSP, MJPEG, and snapshot URLs for integration into Home Assistant, Frigate, or web dashboards.
- **Real-Time Telemetry**: Automatically polls `/cameras` every 3 seconds to display live connection states, active downstream viewer counts, and cumulative ingested bandwidth.

---

## Configuration Specification

`video-amplifier` supports YAML configuration files and environment variable overrides:

```yaml
server:
  http_port: 8080         # HTTP streaming & snapshot port
  rtsp_port: 8554         # Downstream RTSP relay server port
  metrics_port: 9090      # Dedicated Prometheus /metrics and health port
  read_timeout: 10s       # Socket read timeout
  write_timeout: 10s      # Socket write timeout
  log_level: "INFO"       # DEBUG, INFO, WARN, ERROR
  log_format: "text"      # text or json

cameras:
  - id: "front-door"
    name: "Front Door 4K"
    upstream_url: "rtsp://admin:secret@192.168.1.50:554/h264Preview_01_main"
    mode: "always-on"            # "always-on" or "on-demand"
    idle_timeout: "30s"          # Idle grace period before stopping upstream
    client_buffer_size: 120      # Maximum frames queued per client
    retry_interval: "5s"         # Base reconnect interval before backoff
    rtsp_transport: "tcp"        # "tcp", "udp", or "auto"
    client_timeout: "10s"        # Inactivity threshold before dropping client
    latency_watermark: "1500ms"  # Buffer latency threshold before GOP keyframe eviction
    degradation_step: true       # Drop non-ref frames prior to full keyframe eviction
    disconnect_timeout: "3s"     # Time without packets before synthetic keep-alive injection
    synthetic_keepalives: true   # Maintain downstream NVR sessions during upstream dropouts

  - id: "workshop-mjpeg"
    name: "Workshop Overhead"
    upstream_url: "http://192.168.1.60/video.mjpg"
    mode: "on-demand"
    idle_timeout: "30s"
    client_buffer_size: 30
    retry_interval: "5s"
```

### Environment Variable Overrides

| Environment Variable | Description |
|---|---|
| `VIDEO_AMPLIFIER_CONFIG_FILE` | Path to configuration YAML file |
| `VIDEO_AMPLIFIER_SERVER_HTTP_PORT` | HTTP streaming port (default: 8080) |
| `VIDEO_AMPLIFIER_SERVER_RTSP_PORT` | RTSP relay port (default: 8554) |
| `VIDEO_AMPLIFIER_SERVER_METRICS_PORT`| Metrics port (default: 9090) |
| `VIDEO_AMPLIFIER_LOG_LEVEL` | Log level (`DEBUG`, `INFO`, `WARN`, `ERROR`) |
| `VIDEO_AMPLIFIER_LOG_FORMAT` | Log format (`text` or `json`) |

---

## Downstream Endpoints Reference

### HTTP Endpoints (`:8080`)
| Method | Path | Description |
|---|---|---|
| `GET` | `/`, `/ui`, `/dashboard` | **Live Web Interface**: Responsive multi-camera dashboard with live streams & stream URL chips |
| `GET` | `/cameras/{id}/mjpeg` | Multipart MJPEG stream (`multipart/x-mixed-replace; boundary=...`) |
| `GET` | `/cameras/{id}/snapshot.jpg` | Instant cached JPEG single frame (`image/jpeg`) |
| `GET` | `/snapshot.jpg?camera={id}` | Query-based snapshot caching endpoint |
| `POST` | `/cameras/{id}/whep` | **WHEP (WebRTC HTTP Egress)**: Standard SDP offer exchange for ultra-low latency browser playback |
| `DELETE` | `/cameras/{id}/whep/{session}` | Terminate active WHEP WebRTC session |
| `GET` | `/cameras/{id}/fmp4`, `/live.mp4` | **Fragmented MP4**: Chunked transfer stream (`video/mp4`) for MSE and browser `<video>` |
| `GET` | `/cameras/{id}/ws` | **fMP4 over WebSockets**: Low-overhead binary stream for WebSocket-based players |
| `GET`, `POST` | `/cameras/{id}/onvif` | **ONVIF Metadata & Profiles**: Decoupled ONVIF capabilities and profile cache (`application/soap+xml`) |
| `GET` | `/{id}/mjpeg` | Short alias for MJPEG stream |
| `GET` | `/{id}/snapshot.jpg` | Short alias for snapshot |
| `GET` | `/{id}/fmp4` | Short alias for fMP4 stream |
| `GET` | `/{id}/whep` | Short alias for WHEP endpoint |
| `GET` | `/{id}/onvif` | Short alias for ONVIF endpoint |
| `GET` | `/cameras` | JSON list of configured cameras and live metrics |
| `GET` | `/cameras/{id}` | JSON status for a single camera |
| `GET` | `/healthz` | **Detailed JSON Health**: Uptime, per-camera resolution, GOP interval, bitrate, and FPS |
| `GET` | `/readyz` | Readiness probe (`200 OK` / `503 Service Unavailable`) |

### RTSP Endpoints (`:8554`)
| Path | Description |
|---|---|
| `rtsp://<host>:8554/<camera_id>` | High-performance RTSP relay stream for NVRs, VLC, Home Assistant (with timed ONVIF XML metadata track pass-through) |

### Observability Endpoints (`:9090`)
| Method | Path | Description |
|---|---|---|
| `GET` | `/metrics` | Prometheus metrics scrape target (including `upstream_bitrate_bps`, `upstream_fps`, `downstream_client_lag_ms`, `gop_evictions_total`, and `synthetic_frames_injected_total`) |
| `GET` | `/healthz` | Detailed JSON Health endpoint |
| `GET` | `/readyz` | Readiness probe |

---

## Quickstart

### Building from Source

Prerequisites: Go 1.23+

```bash
# Clone the repository
git clone https://github.com/smford/video-amplifier.git
cd video-amplifier

# Run tests and verify zero data races
make test-race

# Build the binary
make build

# Generate a starter configuration file template
./video-amplifier init

# Start video-amplifier
./video-amplifier -config config.yaml
```

### Generating a Configuration File (`init`)

To generate a production-ready, fully commented configuration file with realistic camera examples:

```bash
# Generate config.yaml in the current directory
./video-amplifier init

# Specify a custom destination path
./video-amplifier init -output my-cameras.yaml

# Overwrite an existing configuration file
./video-amplifier init -force

# Print configuration to standard output (e.g. for piping)
./video-amplifier init -stdout
```

### Running with Docker

```bash
# Build the minimal multi-stage image
docker build -t video-amplifier:latest .

# Run container with host ports mapped
docker run -d \
  --name video-amplifier \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -p 8080:8080 \
  -p 8554:8554 \
  -p 9090:9090 \
  video-amplifier:latest
```

---

## Verifying Observability

### 1. Prometheus Scrape Test
```bash
curl -s http://localhost:9090/metrics | grep video_amplifier
```
Example Output:
```text
# HELP video_amplifier_upstream_connected Upstream camera connection state (1 for connected, 0 for disconnected).
# TYPE video_amplifier_upstream_connected gauge
video_amplifier_upstream_connected{camera="Front Door 4K"} 1
video_amplifier_upstream_connected{camera="Workshop Overhead"} 0

# HELP video_amplifier_downstream_active_clients Number of active downstream consumers connected to camera.
# TYPE video_amplifier_downstream_active_clients gauge
video_amplifier_downstream_active_clients{camera="Front Door 4K",protocol="rtsp"} 12
video_amplifier_downstream_active_clients{camera="Front Door 4K",protocol="mjpeg"} 5

# HELP video_amplifier_downstream_dropped_frames_total Total frames dropped for a downstream client due to backpressure.
# TYPE video_amplifier_downstream_dropped_frames_total counter
video_amplifier_downstream_dropped_frames_total{camera="Front Door 4K",client_id="mobile-3f"} 2
```

### 2. Snapshot Caching Test
```bash
curl -s -o /tmp/snapshot.jpg http://localhost:8080/cameras/workshop-mjpeg/snapshot.jpg
file /tmp/snapshot.jpg
# Output: /tmp/snapshot.jpg: JPEG image data
```

---

## License

MIT License. Designed for 24/7 high-reliability camera operations.
