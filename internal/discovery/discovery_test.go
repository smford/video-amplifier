package discovery

import (
	"context"
	"testing"
	"time"
)

func TestParseProbeMatch(t *testing.T) {
	sampleXML := []byte(`<?xml version="1.0" encoding="utf-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:d="http://schemas.xmlsoap.org/ws/2005/04/discovery" xmlns:dn="http://www.onvif.org/ver10/network/wsdl">
  <SOAP-ENV:Header>
    <wsa:MessageID>urn:uuid:12345678-1234-1234-1234-123456789abc</wsa:MessageID>
    <wsa:RelatesTo>urn:uuid:87654321-4321-4321-4321-cba987654321</wsa:RelatesTo>
    <wsa:To>http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous</wsa:To>
    <wsa:Action>http://schemas.xmlsoap.org/ws/2005/04/discovery/ProbeMatches</wsa:Action>
  </SOAP-ENV:Header>
  <SOAP-ENV:Body>
    <d:ProbeMatches>
      <d:ProbeMatch>
        <wsa:EndpointReference>
          <wsa:Address>urn:uuid:camera-uuid-001122334455</wsa:Address>
        </wsa:EndpointReference>
        <d:Types>dn:NetworkVideoTransmitter tds:Device</d:Types>
        <d:Scopes>onvif://www.onvif.org/type/video_encoder onvif://www.onvif.org/name/DrivewayCam</d:Scopes>
        <d:XAddrs>http://192.168.1.150:80/onvif/device_service</d:XAddrs>
      </d:ProbeMatch>
    </d:ProbeMatches>
  </SOAP-ENV:Body>
</SOAP-ENV:Envelope>`)

	dev, err := parseProbeMatch(sampleXML, "192.168.1.150:3702")
	if err != nil {
		t.Fatalf("unexpected error parsing probe match: %v", err)
	}

	if dev.EndpointReference != "urn:uuid:camera-uuid-001122334455" {
		t.Errorf("expected endpoint reference 'urn:uuid:camera-uuid-001122334455', got %q", dev.EndpointReference)
	}
	if len(dev.XAddrs) == 0 || dev.XAddrs[0] != "http://192.168.1.150:80/onvif/device_service" {
		t.Errorf("expected XAddr 'http://192.168.1.150:80/onvif/device_service', got %v", dev.XAddrs)
	}
	if len(dev.Scopes) < 2 {
		t.Errorf("expected scopes, got %v", dev.Scopes)
	}
}

func TestProbeLocalNetwork_Timeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Probing in unit test should gracefully return within timeout without error
	_, err := ProbeLocalNetwork(ctx, 100*time.Millisecond)
	if err != nil {
		t.Logf("Probe error (expected if network multicast unavailable): %v", err)
	}
}
