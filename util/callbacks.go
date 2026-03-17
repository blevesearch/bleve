package util

import (
	zapv17 "github.com/blevesearch/zapx/v17"
)

var WriterHook func(context []byte) (string, func(data []byte) []byte, error)
var ReaderHook func(id string, context []byte) (func(data []byte) ([]byte, error), error)

func init() {
	zapv17.WriterHook = WriterHook
	zapv17.ReaderHook = ReaderHook
}

type FileWriter struct {
	processor func(data []byte) []byte
	context   []byte
	id        string
}

func NewFileWriter(context []byte) (*FileWriter, error) {
	rv := &FileWriter{
		context: context,
	}

	if WriterHook != nil {
		var err error
		rv.id, rv.processor, err = WriterHook(rv.context)
		if err != nil {
			return nil, err
		}
	}

	return rv, nil
}

func (w *FileWriter) Process(data []byte) []byte {
	if w.processor != nil {
		return w.processor(data)
	}
	return data
}

func (w *FileWriter) Id() string {
	return w.id
}

type FileReader struct {
	processor func(data []byte) ([]byte, error)
	id        string
	context   []byte
}

func NewFileReader(id string, context []byte) (*FileReader, error) {
	rv := &FileReader{
		id: id,
	}

	if ReaderHook != nil {
		var err error
		rv.processor, err = ReaderHook(id, context)
		if err != nil {
			return nil, err
		}
	}

	return rv, nil
}

func (r *FileReader) Process(data []byte) ([]byte, error) {
	if r.processor != nil {
		return r.processor(data)
	}
	return data, nil
}

func (r *FileReader) Id() string {
	return r.id
}
