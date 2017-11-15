package document

import (
	"net"
	"testing"
)

func TestIPField(t *testing.T) {
	nf := NewIPField("ip", []uint64{}, net.ParseIP("192.168.0.1"))
	numTokens, tokenFreqs := nf.Analyze()
	if numTokens != 1 {
		t.Errorf("expected 1 token")
	}
	if len(tokenFreqs) != 1 {
		t.Errorf("expected 1 token freqs")
	}
}
