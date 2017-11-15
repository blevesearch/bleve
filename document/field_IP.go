package document

import (
	"fmt"
	"net"

	"github.com/blevesearch/bleve/analysis"
)

const DefaultIPIndexingOptions = StoreField | IndexField

type IPField struct {
	name              string
	arrayPositions    []uint64
	options           IndexingOptions
	value             []byte
	numPlainTextBytes uint64
}

func (n *IPField) Name() string {
	return n.name
}

func (n *IPField) ArrayPositions() []uint64 {
	return n.arrayPositions
}

func (n *IPField) Options() IndexingOptions {
	return n.options
}

func (n *IPField) Analyze() (int, analysis.TokenFrequencies) {
	tokens := make(analysis.TokenStream, 0)
	tokens = append(tokens, &analysis.Token{
		Start:    0,
		End:      len(n.value),
		Term:     n.value,
		Position: 1,
		Type:     analysis.Numeric,
	})

	//original, err := n.value
	//if err == nil {
	//
	//	shift := DefaultPrecisionStep
	//	//TODO: break to bytes
	//	for shift < 128 {
	//		shiftEncoded, err := numeric.NewPrefixCodedInt64(original, shift)
	//		if err != nil {
	//			break
	//		}
	//		token := analysis.Token{
	//			Start:    0,
	//			End:      len(shiftEncoded),
	//			Term:     shiftEncoded,
	//			Position: 1,
	//			Type:     analysis.Numeric,
	//		}
	//		tokens = append(tokens, &token)
	//		shift += DefaultPrecisionStep
	//	}
	//}

	fieldLength := len(tokens)
	tokenFreqs := analysis.TokenFrequency(tokens, n.arrayPositions, n.options.IncludeTermVectors())
	return fieldLength, tokenFreqs
}

func (n *IPField) Value() []byte {
	return n.value
}

func (n *IPField) GoString() string {
	var sip net.IP
	sip.UnmarshalText(n.value)
	return fmt.Sprintf("&document.NumericField{Name:%s, Options: %s, Value: %s}", n.name, n.options, sip)
}

func (n *IPField) NumPlainTextBytes() uint64 {
	return n.numPlainTextBytes
}

func NewIPFieldFromBytes(name string, arrayPositions []uint64, value []byte) *IPField {
	return &IPField{
		name:              name,
		arrayPositions:    arrayPositions,
		value:             value,
		options:           DefaultIPIndexingOptions,
		numPlainTextBytes: uint64(len(value)),
	}
}

func NewIPField(name string, arrayPositions []uint64, value net.IP) *IPField {
	return NewIPFieldFromBytes(name, arrayPositions, value)
}

func NewIPFieldWithIndexingOptions(name string, arrayPositions []uint64, value []byte, options IndexingOptions) *IPField {
	return &IPField{
		name:              name,
		arrayPositions:    arrayPositions,
		value:             value,
		options:           options,
		numPlainTextBytes: uint64(len(value)),
	}
}
