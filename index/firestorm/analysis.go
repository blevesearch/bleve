//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package firestorm

import (
	"math"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
)

func (f *Firestorm) Analyze(d *document.Document) *index.AnalysisResult {

	rv := &index.AnalysisResult{
		DocID: d.ID,
		Rows:  make([]index.IndexRow, 0, 100),
	}

	docIDBytes := []byte(d.ID)

	// information we collate as we merge fields with same name
	fieldTermFreqs := make(map[uint16]analysis.TokenFrequencies)
	fieldLengths := make(map[uint16]int)
	fieldIncludeTermVectors := make(map[uint16]bool)
	fieldNames := make(map[uint16]string)

	for _, field := range d.Fields {
		fieldIndex, newFieldRow := f.fieldIndexOrNewRow(field.Name())
		if newFieldRow != nil {
			rv.Rows = append(rv.Rows, newFieldRow)
		}
		fieldNames[fieldIndex] = field.Name()

		// add the _id row
		rv.Rows = append(rv.Rows, NewTermFreqRow(0, nil, docIDBytes, d.Number, 0, 0, nil))

		if field.Options().IsIndexed() {
			fieldLength, tokenFreqs := field.Analyze()
			existingFreqs := fieldTermFreqs[fieldIndex]
			if existingFreqs == nil {
				fieldTermFreqs[fieldIndex] = tokenFreqs
			} else {
				existingFreqs.MergeAll(field.Name(), tokenFreqs)
				fieldTermFreqs[fieldIndex] = existingFreqs
			}
			fieldLengths[fieldIndex] += fieldLength
			fieldIncludeTermVectors[fieldIndex] = field.Options().IncludeTermVectors()
		}

		if field.Options().IsStored() {
			storeRow := f.storeField(docIDBytes, d.Number, field, fieldIndex)
			rv.Rows = append(rv.Rows, storeRow)
		}
	}

	// walk through the collated information and proccess
	// once for each indexed field (unique name)
	for fieldIndex, tokenFreqs := range fieldTermFreqs {
		fieldLength := fieldLengths[fieldIndex]
		includeTermVectors := fieldIncludeTermVectors[fieldIndex]

		// see if any of the composite fields need this
		for _, compositeField := range d.CompositeFields {
			compositeField.Compose(fieldNames[fieldIndex], fieldLength, tokenFreqs)
		}

		// encode this field
		indexRows := f.indexField(docIDBytes, d.Number, includeTermVectors, fieldIndex, fieldLength, tokenFreqs)
		rv.Rows = append(rv.Rows, indexRows...)
	}

	// now index the composite fields
	for _, compositeField := range d.CompositeFields {
		fieldIndex, newFieldRow := f.fieldIndexOrNewRow(compositeField.Name())
		if newFieldRow != nil {
			rv.Rows = append(rv.Rows, newFieldRow)
		}
		if compositeField.Options().IsIndexed() {
			fieldLength, tokenFreqs := compositeField.Analyze()
			// encode this field
			indexRows := f.indexField(docIDBytes, d.Number, compositeField.Options().IncludeTermVectors(), fieldIndex, fieldLength, tokenFreqs)
			rv.Rows = append(rv.Rows, indexRows...)
		}
	}

	return rv
}

func (f *Firestorm) indexField(docID []byte, docNum uint64, includeTermVectors bool, fieldIndex uint16, fieldLength int, tokenFreqs analysis.TokenFrequencies) []index.IndexRow {

	tfrs := make([]TermFreqRow, len(tokenFreqs))

	fieldNorm := float32(1.0 / math.Sqrt(float64(fieldLength)))

	if !includeTermVectors {
		rows := make([]index.IndexRow, len(tokenFreqs))
		i := 0
		for _, tf := range tokenFreqs {
			rows[i] = InitTermFreqRow(&tfrs[i], fieldIndex, tf.Term, docID, docNum, uint64(tf.Frequency()), fieldNorm, nil)
			i++
		}
		return rows
	}

	rows := make([]index.IndexRow, 0, len(tokenFreqs))
	i := 0
	for _, tf := range tokenFreqs {
		tv, newFieldRows := f.termVectorsFromTokenFreq(fieldIndex, tf)
		rows = append(rows, newFieldRows...)
		rows = append(rows, InitTermFreqRow(&tfrs[i], fieldIndex, tf.Term, docID, docNum, uint64(tf.Frequency()), fieldNorm, tv))
		i++
	}
	return rows
}

func (f *Firestorm) termVectorsFromTokenFreq(field uint16, tf *analysis.TokenFreq) ([]*TermVector, []index.IndexRow) {
	rv := make([]*TermVector, len(tf.Locations))
	newFieldRows := make([]index.IndexRow, 0)

	for i, l := range tf.Locations {
		var newFieldRow *FieldRow
		fieldIndex := field
		if l.Field != "" {
			// lookup correct field
			fieldIndex, newFieldRow = f.fieldIndexOrNewRow(l.Field)
			if newFieldRow != nil {
				newFieldRows = append(newFieldRows, newFieldRow)
			}
		}
		tv := NewTermVector(fieldIndex, uint64(l.Position), uint64(l.Start), uint64(l.End), l.ArrayPositions)
		rv[i] = tv
	}

	return rv, newFieldRows
}

func (f *Firestorm) storeField(docID []byte, docNum uint64, field document.Field, fieldIndex uint16) index.IndexRow {
	fieldValue := make([]byte, 1+len(field.Value()))
	fieldValue[0] = encodeFieldType(field)
	copy(fieldValue[1:], field.Value())
	storedRow := NewStoredRow(docID, docNum, fieldIndex, field.ArrayPositions(), fieldValue)
	return storedRow
}

func encodeFieldType(f document.Field) byte {
	fieldType := byte('x')
	switch f.(type) {
	case *document.TextField:
		fieldType = 't'
	case *document.NumericField:
		fieldType = 'n'
	case *document.DateTimeField:
		fieldType = 'd'
	case *document.CompositeField:
		fieldType = 'c'
	}
	return fieldType
}
