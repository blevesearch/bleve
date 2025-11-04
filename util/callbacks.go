package util

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"

	zapv16 "github.com/blevesearch/zapx/v16"
)

// Variables used for development and testing purposes
// var keys = map[string][]byte{}
// var cbLock = sync.RWMutex{}
// var latestCallbackId string

var WriterCallbackGetter = func() (string, func(data, counter []byte) ([]byte, error), error) {
	return "", func(data, counter []byte) ([]byte, error) {
		return data, nil
	}, nil
}

var ReaderCallbackGetter = func(cbId string) (func(data []byte) ([]byte, error), error) {
	return func(data []byte) ([]byte, error) {
		return data, nil
	}, nil
}

var CounterGetter = func() ([]byte, error) {
	return nil, nil
}

func init() {
	// Variables used for development and testing purposes
	encryptionKey := make([]byte, 32)
	if _, err := rand.Read(encryptionKey); err != nil {
		panic("failed to generate AES key: " + err.Error())
	}

	key := make([]byte, 32)
	keyId := "test-key-id"

	if _, err := rand.Read(key); err != nil {
		panic("Failed to generate random key: " + err.Error())
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		panic("Failed to create AES cipher: " + err.Error())
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		panic("Failed to create AES GCM: " + err.Error())
	}

	CounterGetter = func() ([]byte, error) {
		counter := make([]byte, 12)
		if _, err := rand.Read(counter); err != nil {
			return nil, err
		}
		return counter, nil
	}

	writerCallback := func(data, counter []byte) ([]byte, error) {
		ciphertext := aesgcm.Seal(nil, counter, data, nil)
		result := append(ciphertext, counter...)

		// For testing purposes only
		result = append(append([]byte("EncStart"), result...), []byte("EncEnd")...)

		return result, nil
	}

	readerCallback := func(data []byte) ([]byte, error) {
		// For testing purposes only
		data = bytes.TrimPrefix(data, []byte("EncStart"))
		data = bytes.TrimSuffix(data, []byte("EncEnd"))

		if len(data) < 12 {
			return nil, fmt.Errorf("ciphertext too short")
		}

		counter := data[len(data)-12:]
		ciphertext := data[:len(data)-12]
		plaintext, err := aesgcm.Open(nil, counter, ciphertext, nil)
		if err != nil {
			return nil, err
		}
		return plaintext, nil
	}

	WriterCallbackGetter = func() (string, func(data []byte, counter []byte) ([]byte, error), error) {
		return keyId, writerCallback, nil
	}

	ReaderCallbackGetter = func(id string) (func(data []byte) ([]byte, error), error) {
		if id != keyId {
			return nil, fmt.Errorf("unknown callback ID: %s", id)
		}
		return readerCallback, nil
	}

	zapv16.WriterCallbackGetter = WriterCallbackGetter
	zapv16.ReaderCallbackGetter = ReaderCallbackGetter
	zapv16.CounterGetter = CounterGetter
}

type FileWriter struct {
	writerCB func(data, counter []byte) ([]byte, error)
	counter  []byte
	id       string
}

func NewFileWriter() (*FileWriter, error) {
	var err error
	rv := &FileWriter{}
	rv.id, rv.writerCB, err = WriterCallbackGetter()
	if err != nil {
		return nil, err
	}
	rv.counter, err = CounterGetter()
	if err != nil {
		return nil, err
	}

	return rv, nil
}

func (w *FileWriter) Process(data []byte) ([]byte, error) {
	if w.writerCB != nil {
		w.incrementCounter()
		return w.writerCB(data, w.counter)
	}
	return data, nil
}

func (w *FileWriter) incrementCounter() {
	if w.counter != nil {
		for i := len(w.counter) - 1; i >= 0; i-- {
			if w.counter[i] < 255 {
				w.counter[i]++
				return
			}
			w.counter[i] = 0
		}
	}
}

func (w *FileWriter) Id() string {
	return w.id
}

type FileReader struct {
	readerCB func(data []byte) ([]byte, error)
	id       string
}

func NewFileReader(cbId string) (*FileReader, error) {
	readerCB, err := ReaderCallbackGetter(cbId)
	if err != nil {
		return nil, err
	}

	return &FileReader{
		readerCB: readerCB,
		id:       cbId,
	}, nil
}

func (r *FileReader) Process(data []byte) ([]byte, error) {
	if r.readerCB != nil {
		return r.readerCB(data)
	}
	return data, nil
}

func (r *FileReader) Id() string {
	return r.id
}
