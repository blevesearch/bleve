package document

import (
	"bytes"
	"net"
	"testing"
)

func TestIPField(t *testing.T) {
	nf := NewIpField("ip", []uint64{}, net.IPv4(192, 168, 1, 1))
	nf.Analyze()
	if nf.length != 1 {
		t.Errorf("expected 1 token")
	}
	if len(nf.value) != 16 {
		t.Errorf("stored value should be in 16 byte ipv6 format")
	}
	if !bytes.Equal(nf.value, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 168, 1, 1}) {
		t.Errorf("wrong value stored, expected 192.168.1.1, got %q", nf.value.String())
	}
	if len(nf.frequencies) != 1 {
		t.Errorf("expected 1 token freqs")
	}
}
