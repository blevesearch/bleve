package document

import (
	"github.com/couchbaselabs/bleve/analysis"
)

const DEFAULT_COMPOSITE_INDEXING_OPTIONS = INDEX_FIELD

type CompositeField struct {
	name                 string
	includedFields       map[string]bool
	excludedFields       map[string]bool
	defaultInclude       bool
	options              IndexingOptions
	totalLength          int
	compositeFrequencies analysis.TokenFrequencies
}

func NewCompositeField(name string, defaultInclude bool, include []string, exclude []string) *CompositeField {
	return NewCompositeFieldWithIndexingOptions(name, defaultInclude, include, exclude, DEFAULT_COMPOSITE_INDEXING_OPTIONS)
}

func NewCompositeFieldWithIndexingOptions(name string, defaultInclude bool, include []string, exclude []string, options IndexingOptions) *CompositeField {
	rv := &CompositeField{
		name:           name,
		options:        options,
		defaultInclude: defaultInclude,
		includedFields: make(map[string]bool, len(include)),
		excludedFields: make(map[string]bool, len(exclude)),
	}

	for _, i := range include {
		rv.includedFields[i] = true
	}
	for _, e := range exclude {
		rv.excludedFields[e] = true
	}

	return rv
}

func (c *CompositeField) Name() string {
	return c.name
}

func (c *CompositeField) Options() IndexingOptions {
	return c.options
}

func (c *CompositeField) Analyze() (int, analysis.TokenFrequencies) {
	return c.totalLength, c.compositeFrequencies
}

func (c *CompositeField) Value() []byte {
	return []byte{}
}

func (c *CompositeField) Compose(field string, length int, freq analysis.TokenFrequencies) {
	shouldInclude := c.defaultInclude
	_, fieldShouldBeIncluded := c.includedFields[field]
	if fieldShouldBeIncluded {
		shouldInclude = true
	}
	_, fieldShouldBeExcluded := c.excludedFields[field]
	if fieldShouldBeExcluded {
		shouldInclude = false
	}

	if shouldInclude {
		c.totalLength += length
		c.compositeFrequencies = c.compositeFrequencies.MergeAll(field, freq)
	}
}
