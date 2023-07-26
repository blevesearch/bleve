package document

import (
	"fmt"
	"reflect"

	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeDenseVectorField int

func init() {
	var f DenseVectorField
	reflectStaticSizeDenseVectorField = int(reflect.TypeOf(f).Size())
}

const DefaultDenseVectorIndexingOptions = index.IndexField

type DenseVectorField struct {
	name              string
	dims              int
	similarity        string
	options           index.FieldIndexingOptions
	value             []float32
	numPlainTextBytes uint64
}

func (n *DenseVectorField) Size() int {
	return reflectStaticSizeDenseVectorField + size.SizeOfPtr +
		len(n.name) +
		int(numBytesSingleFloats(n.value))
}

func (n *DenseVectorField) Name() string {
	return n.name
}

func (n *DenseVectorField) ArrayPositions() []uint64 {
	return nil
}

func (n *DenseVectorField) Options() index.FieldIndexingOptions {
	return n.options
}

func (n *DenseVectorField) NumPlainTextBytes() uint64 {
	return n.numPlainTextBytes
}

func (n *DenseVectorField) AnalyzedLength() int {
	// dense vectors aren't analyzed
	return 0
}

func (n *DenseVectorField) EncodedFieldType() byte {
	return 'v'
}

func (n *DenseVectorField) AnalyzedTokenFrequencies() index.TokenFrequencies {
	// dense vectors aren't analyzed
	return nil
}

func (n *DenseVectorField) Analyze() {
	// dense vectors aren't analyzed
}

func (n *DenseVectorField) Value() []byte {
	return nil
}

func (n *DenseVectorField) GoString() string {
	return fmt.Sprintf("&document.DenseVectorField{Name:%s, Options: %s, Value: %+v}",
		n.name, n.options, n.value)
}

// For the sake of not polluting the API, we are keeping arrayPositions as a
// parameter, but it is not used.
func NewDenseVectorField(name string, arrayPositions []uint64,
	denseVector []float32, dims int, similarity string) *DenseVectorField {
	return NewDenseVectorFieldWithIndexingOptions(name, arrayPositions,
		denseVector, dims, similarity, DefaultDenseVectorIndexingOptions)
}

// For the sake of not polluting the API, we are keeping arrayPositions as a
// parameter, but it is not used.
func NewDenseVectorFieldWithIndexingOptions(name string, arrayPositions []uint64,
	denseVector []float32, dims int, similarity string, options index.FieldIndexingOptions) *DenseVectorField {
	options = options | DefaultDenseVectorIndexingOptions

	return &DenseVectorField{
		name:              name,
		dims:              dims,
		similarity:        similarity,
		options:           options,
		value:             denseVector,
		numPlainTextBytes: numBytesSingleFloats(denseVector),
	}
}

func numBytesSingleFloats(value []float32) uint64 {
	return uint64(len(value) * 4)
}

// DenseVector is an implementation of the index.DenseVectorField interface.
func (n *DenseVectorField) DenseVector() ([]float32, int, string) {
	return n.value, n.dims, n.similarity
}
