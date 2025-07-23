package util

import (
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

var WriterCallbackGetterWithId = func(cbId string) (func(data, counter []byte) ([]byte, error), error) {
	return func(data, counter []byte) ([]byte, error) {
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
	// encryptionKey := make([]byte, 32)
	// if _, err := rand.Read(encryptionKey); err != nil {
	// 	panic("failed to generate AES key: " + err.Error())
	// }

	// latestCallbackId = "exampleCallback"
	// keys[latestCallbackId] = encryptionKey

	// latestCallbackId = "exampleCallback"

	// WriterCallbackGetter = func() (string, func(data, counter []byte) ([]byte, error), error) {
	// 	cbLock.RLock()
	// 	if latestCallbackId == "" {
	// 		return "", func(data []byte, _ []byte) ([]byte, error) {
	// 			return data, nil
	// 		}, nil
	// 	}
	// 	keyCopy := make([]byte, 32)
	// 	keyIdCopy := latestCallbackId
	// 	if key, exists := keys[latestCallbackId]; exists {
	// 		copy(keyCopy, key)
	// 	}
	// 	cbLock.RUnlock()

	// 	block, err := aes.NewCipher(keyCopy)
	// 	if err != nil {
	// 		return "", nil, err
	// 	}
	// 	aesgcm, err := cipher.NewGCM(block)
	// 	if err != nil {
	// 		return "", nil, err
	// 	}

	// 	return keyIdCopy, func(data, counter []byte) ([]byte, error) {
	// 		ciphertext := aesgcm.Seal(nil, counter, data, nil)
	// 		result := append(ciphertext, counter...)
	// 		return result, nil
	// 	}, nil
	// }

	// ReaderCallbackGetter = func(cbId string) (func(data []byte) ([]byte, error), error) {
	// 	cbLock.RLock()
	// 	keyCopy := make([]byte, 32)
	// 	if key, exists := keys[cbId]; exists {
	// 		copy(keyCopy, key)
	// 	}
	// 	cbLock.RUnlock()

	// 	if len(keyCopy) == 0 {
	// 		return func(data []byte) ([]byte, error) {
	// 			return data, nil
	// 		}, nil
	// 	} else {
	// 		block, err := aes.NewCipher(keyCopy)
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		aesgcm, err := cipher.NewGCM(block)
	// 		if err != nil {
	// 			return nil, err
	// 		}

	// 		return func(data []byte) ([]byte, error) {
	// 			if len(data) < 12 {
	// 				return nil, fmt.Errorf("ciphertext too short")
	// 			}

	// 			nonce := data[len(data)-12:]
	// 			ciphertext := data[:len(data)-12]

	// 			plaintext, err := aesgcm.Open(nil, nonce, ciphertext, nil)
	// 			if err != nil {
	// 				return nil, fmt.Errorf("decryption failed: %w", err)
	// 			}

	// 			return plaintext, nil
	// 		}, nil
	// 	}
	// }

	// CounterGetter = func() ([]byte, error) {
	// 	nonce := make([]byte, 12) // GCM standard
	// 	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
	// 		return nil, err
	// 	}
	// 	return nonce, nil
	// }

	zapv16.WriterCallbackGetter = WriterCallbackGetter
	zapv16.ReaderCallbackGetter = ReaderCallbackGetter
	zapv16.CounterGetter = CounterGetter
}

// Function used for development and testing purposes
// func SetNewCallback(callbackId string, key []byte) {
// 	if callbackId != "" {
// 		cbLock.Lock()
// 		keys[callbackId] = key
// 		latestCallbackId = callbackId
// 		cbLock.Unlock()
// 	}
// }

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

func NewFileWriterWithId(cbId string) (*FileWriter, error) {
	writerCB, err := WriterCallbackGetterWithId(cbId)
	if err != nil {
		return nil, err
	}

	counter, err := CounterGetter()
	if err != nil {
		return nil, err
	}

	return &FileWriter{
		writerCB: writerCB,
		counter:  counter,
		id:       cbId,
	}, nil
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
