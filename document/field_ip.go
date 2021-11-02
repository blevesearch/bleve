//  Copyright (c) 2021 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package document

import (
	"fmt"
	"net"
	"reflect"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeIpField int

func init() {
	var f IpField
	reflectStaticSizeIpField = int(reflect.TypeOf(f).Size())
}

const DefaultIpIndexingOptions = index.StoreField | index.IndexField | index.DocValues | index.IncludeTermVectors

type IpField struct {
	name              string
	arrayPositions    []uint64
	options           index.FieldIndexingOptions
	value             net.IP
	numPlainTextBytes uint64
	length            int
	frequencies       index.TokenFrequencies
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

func (b *IpField) Options() index.FieldIndexingOptions {
	return b.options
}

func (n *IpField) EncodedFieldType() byte {
	return 'i'
}

func (n *IpField) AnalyzedLength() int {
	return n.length
}

func (n *IpField) AnalyzedTokenFrequencies() index.TokenFrequencies {
	return n.frequencies
}

func (b *IpField) Analyze() {

	tokens := analysis.TokenStream{
		&analysis.Token{
			Start:    0,
			End:      len(b.value),
			Term:     b.value,
			Position: 1,
			Type:     analysis.Ip,
		},
	}
	b.length = 1
	b.frequencies = analysis.TokenFrequency(tokens, b.arrayPositions, b.options)
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

func NewIpFieldWithIndexingOptions(name string, arrayPositions []uint64, b net.IP, options index.FieldIndexingOptions) *IpField {
	v := b.To16()

	return &IpField{
		name:              name,
		arrayPositions:    arrayPositions,
		value:             v,
		options:           options,
		numPlainTextBytes: net.IPv6len,
	}
}
