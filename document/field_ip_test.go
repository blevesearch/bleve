package document

import (
	"net"
	"testing"
)

func TestIPField(t *testing.T) {
	nf := NewIpField("age", []uint64{}, net.IPv4(192,168,1,1))
	nf.Analyze()
	if nf.length != 1 {
		t.Errorf("expected 1 token")
	}
	if len(nf.frequencies) != 1 {
		t.Errorf("expected 1 token freqs")
	}
}
