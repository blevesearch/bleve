package document

import "testing"

func TestGeoPointField(t *testing.T) {
	gf := NewGeoPointField("loc", []uint64{}, 0.0015, 0.0015)
	gf.Analyze()
	numTokens := gf.AnalyzedLength()
	tokenFreqs := gf.AnalyzedTokenFrequencies()
	if numTokens != 8 {
		t.Errorf("expected 8 tokens, got %d", numTokens)
	}
	if len(tokenFreqs) != 8 {
		t.Errorf("expected 8 token freqs")
	}
}
