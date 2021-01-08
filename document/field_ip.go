
package document

import (
	"fmt"
	"net"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/size"
)

var reflectStaticSizeIpField int

func init() {
	reflectStaticSizeIpField = net.IPv6len
}

const DefaultIpIndexingOptions = StoreField | IndexField | DocValues

type IpField struct {
	name              string
	arrayPositions    []uint64
	options           IndexingOptions
	value             []byte
	numPlainTextBytes uint64
}

func (b *IpField) Size() int {
	return reflectStaticSizeIpField + size.SizeOfPtr +
		len(b.name) +
		len(b.arrayPositions)*size.SizeOfUint64 +
		len(b.value)
}

func (b *IpField) Name() string {
	return b.name
}

func (b *IpField) ArrayPositions() []uint64 {
	return b.arrayPositions
}

func (b *IpField) Options() IndexingOptions {
	return b.options
}

func (b *IpField) Analyze() (int, analysis.TokenFrequencies) {
	tokens := analysis.TokenStream{
		&analysis.Token{
			Start:    0,
			End:      len(b.value),
			Term:     b.value,
			Position: 1,
			Type:     analysis.Ip,
		},
	}
	fieldLength := 1
	tokenFreqs := analysis.TokenFrequency(tokens, b.arrayPositions, b.options.IncludeTermVectors())
	return fieldLength, tokenFreqs
}

func (b *IpField) Value() []byte {
	return b.value
}

func (b *IpField) Ip() (net.IP, error) {
	return net.IP(b.value), nil
}

func (b *IpField) GoString() string {
	return fmt.Sprintf("&document.IpField{Name:%s, Options: %s, Value: %s}", b.name, b.options, net.IP(b.value))
}

func (b *IpField) NumPlainTextBytes() uint64 {
	return b.numPlainTextBytes
}

func NewIpFieldFromBytes(name string, arrayPositions []uint64, value []byte) *IpField {
	return &IpField{
		name:              name,
		arrayPositions:    arrayPositions,
		value:             value,
		options:           DefaultNumericIndexingOptions,
		numPlainTextBytes: uint64(len(value)),
	}
}

func NewIpField(name string, arrayPositions []uint64, v net.IP) *IpField {
	return NewIpFieldWithIndexingOptions(name, arrayPositions, v, DefaultIpIndexingOptions)
}

func NewIpFieldWithIndexingOptions(name string, arrayPositions []uint64, b net.IP, options IndexingOptions) *IpField {
	numPlainTextBytes := 16
	v := make([]byte, numPlainTextBytes)
	copy(v, b)

	return &IpField{
		name:              name,
		arrayPositions:    arrayPositions,
		value:             v,
		options:           options,
		numPlainTextBytes: uint64(numPlainTextBytes),
	}
}
