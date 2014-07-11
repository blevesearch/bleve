package document

import (
	"github.com/couchbaselabs/bleve/analysis"
)

type FieldMapping struct {
	Name     string
	Options  IndexingOptions
	Analyzer *analysis.Analyzer
}

type Mapping map[string]*FieldMapping
