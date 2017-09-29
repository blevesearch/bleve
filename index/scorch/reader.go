package scorch

import (
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
)

type Reader struct {
	root *IndexSnapshot
}

func (r *Reader) TermFieldReader(term []byte, field string, includeFreq,
	includeNorm, includeTermVectors bool) (index.TermFieldReader, error) {
	return r.root.TermFieldReader(term, field, includeFreq, includeNorm, includeTermVectors)
}

// DocIDReader returns an iterator over all doc ids
// The caller must close returned instance to release associated resources.
func (r *Reader) DocIDReaderAll() (index.DocIDReader, error) {
	return r.root.DocIDReaderAll()
}

func (r *Reader) DocIDReaderOnly(ids []string) (index.DocIDReader, error) {
	return r.root.DocIDReaderOnly(ids)
}

func (r *Reader) FieldDict(field string) (index.FieldDict, error) {
	return r.root.FieldDict(field)
}

// FieldDictRange is currently defined to include the start and end terms
func (r *Reader) FieldDictRange(field string, startTerm []byte,
	endTerm []byte) (index.FieldDict, error) {
	return r.root.FieldDictRange(field, startTerm, endTerm)
}

func (r *Reader) FieldDictPrefix(field string,
	termPrefix []byte) (index.FieldDict, error) {
	return r.root.FieldDictPrefix(field, termPrefix)
}

func (r *Reader) Document(id string) (*document.Document, error) {
	return r.root.Document(id)
}
func (r *Reader) DocumentVisitFieldTerms(id index.IndexInternalID, fields []string,
	visitor index.DocumentFieldTermVisitor) error {
	panic("document visit field terms not implemented")
}

func (r *Reader) Fields() ([]string, error) {
	return r.root.Fields()
}

func (r *Reader) GetInternal(key []byte) ([]byte, error) {
	return r.root.GetInternal(key)
}

func (r *Reader) DocCount() (uint64, error) {
	return r.root.DocCount()
}

func (r *Reader) ExternalID(id index.IndexInternalID) (string, error) {
	return r.root.ExternalID(id)
}

func (r *Reader) InternalID(id string) (index.IndexInternalID, error) {
	return r.root.InternalID(id)
}

func (r *Reader) DumpAll() chan interface{} {
	panic("dumpall")
}

func (r *Reader) DumpDoc(id string) chan interface{} {
	panic("dumpdoc")
}

func (r *Reader) DumpFields() chan interface{} {
	panic("dumpfields")
}

func (r *Reader) Close() error {
	return nil
}
