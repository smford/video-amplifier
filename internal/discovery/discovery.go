package discovery

import (
	"context"
	"encoding/xml"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
)

// DiscoveredDevice represents an ONVIF/WS-Discovery device located on the local network.
type DiscoveredDevice struct {
	EndpointReference string   `json:"endpoint_reference"`
	XAddrs            []string `json:"xaddrs"`
	Types             []string `json:"types"`
	Scopes            []string `json:"scopes"`
	Address           string   `json:"address"`
}

const (
	wsdMulticastAddress = "239.255.255.250:3702"
)

// ProbeLocalNetwork broadcasts a standard WS-Discovery Probe payload on the local network
// and collects matching responses from ONVIF IP cameras until the specified timeout expires.
func ProbeLocalNetwork(ctx context.Context, timeout time.Duration) ([]DiscoveredDevice, error) {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	raddr, err := net.ResolveUDPAddr("udp4", wsdMulticastAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve WS-Discovery multicast address: %w", err)
	}

	conn, err := net.ListenUDP("udp4", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to bind UDP socket for discovery: %w", err)
	}
	defer conn.Close()

	msgID := uuid.New().String()
	probeMsg := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<Envelope xmlns:tds="http://www.onvif.org/ver10/device/wsdl" xmlns="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing">
  <Header>
    <wsa:MessageID>urn:uuid:%s</wsa:MessageID>
    <wsa:To>urn:schemas-xmlsoap-org:ws:2005:04:discovery</wsa:To>
    <wsa:Action>http://schemas.xmlsoap.org/ws/2005/04/discovery/Probe</wsa:Action>
  </Header>
  <Body>
    <Probe xmlns="http://schemas.xmlsoap.org/ws/2005/04/discovery">
      <Types>tds:Device</Types>
    </Probe>
  </Body>
</Envelope>`, msgID)

	if _, err := conn.WriteTo([]byte(probeMsg), raddr); err != nil {
		return nil, fmt.Errorf("failed to send WS-Discovery probe packet: %w", err)
	}
	if localAddr, err := net.ResolveUDPAddr("udp4", "127.0.0.1:3702"); err == nil {
		_, _ = conn.WriteTo([]byte(probeMsg), localAddr)
	}

	deadline := time.Now().Add(timeout)
	_ = conn.SetReadDeadline(deadline)

	var devices []DiscoveredDevice
	seen := make(map[string]bool)
	buf := make([]byte, 8192)

	for {
		select {
		case <-ctx.Done():
			return devices, ctx.Err()
		default:
		}

		n, src, err := conn.ReadFrom(buf)
		if err != nil {
			// Read deadline expired or socket closed
			break
		}

		devs, parseErr := parseProbeMatches(buf[:n], src.String())
		if parseErr == nil {
			for _, dev := range devs {
				if dev.EndpointReference != "" && !seen[dev.EndpointReference] {
					seen[dev.EndpointReference] = true
					devices = append(devices, dev)
				}
			}
		}
	}

	return devices, nil
}

// Minimal XML structs for parsing WS-Discovery ProbeMatches
type probeEnvelope struct {
	Body probeBody `xml:"Body"`
}

type probeBody struct {
	ProbeMatches probeMatches `xml:"ProbeMatches"`
}

type probeMatches struct {
	ProbeMatch []probeMatch `xml:"ProbeMatch"`
}

type probeMatch struct {
	EndpointReference struct {
		Address string `xml:"Address"`
	} `xml:"EndpointReference"`
	Types  string `xml:"Types"`
	Scopes string `xml:"Scopes"`
	XAddrs string `xml:"XAddrs"`
}

// parseProbeMatches extracts all ProbeMatch entries from a WS-Discovery ProbeMatches response envelope.
func parseProbeMatches(data []byte, srcAddr string) ([]DiscoveredDevice, error) {
	var env probeEnvelope
	if err := xml.Unmarshal(data, &env); err != nil {
		return nil, err
	}

	if len(env.Body.ProbeMatches.ProbeMatch) == 0 {
		return nil, fmt.Errorf("no probe matches found in response")
	}

	var devices []DiscoveredDevice
	for _, match := range env.Body.ProbeMatches.ProbeMatch {
		xaddrs := strings.Fields(match.XAddrs)
		types := strings.Fields(match.Types)
		scopes := strings.Fields(match.Scopes)

		devices = append(devices, DiscoveredDevice{
			EndpointReference: match.EndpointReference.Address,
			XAddrs:            xaddrs,
			Types:             types,
			Scopes:            scopes,
			Address:           srcAddr,
		})
	}

	return devices, nil
}

// parseProbeMatch parses the first ProbeMatch entry for backwards compatibility.
func parseProbeMatch(data []byte, srcAddr string) (DiscoveredDevice, error) {
	devs, err := parseProbeMatches(data, srcAddr)
	if err != nil {
		return DiscoveredDevice{}, err
	}
	return devs[0], nil
}
