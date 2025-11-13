package util

var WriterHook func(context []byte) (string, func(data []byte) []byte, error)

var ReaderHook func(id string, context []byte) (func(data []byte) ([]byte, error), error)

func init() {
	// Variables used for development and testing purposes
	// encryptionKey := make([]byte, 32)
	// if _, err := rand.Read(encryptionKey); err != nil {
	// 	panic("failed to generate AES key: " + err.Error())
	// }

	// key := make([]byte, 32)
	// keyId := "test-key-id"
	// label := []byte("search")

	// if _, err := rand.Read(key); err != nil {
	// 	panic("Failed to generate random key: " + err.Error())
	// }

	// WriterHook = func(context []byte) (string, func(data []byte) []byte, error) {

	// 	derivedKey := make([]byte, 32)
	// 	derivedKey, err := crypto.OpenSSLKBKDFDeriveKey(key, label, context, derivedKey, "SHA2-256", "")
	// 	if err != nil {
	// 		return "", nil, err
	// 	}

	// 	block, err := aes.NewCipher(derivedKey)
	// 	if err != nil {
	// 		panic("Failed to create AES cipher: " + err.Error())
	// 	}

	// 	aesgcm, err := cipher.NewGCM(block)
	// 	if err != nil {
	// 		panic("Failed to create AES GCM: " + err.Error())
	// 	}

	// 	nonce := make([]byte, 12)
	// 	if _, err := rand.Read(nonce); err != nil {
	// 		panic("Failed to generate random nonce: " + err.Error())
	// 	}

	// 	writerCallback := func(data []byte) []byte {
	// 		ciphertext := aesgcm.Seal(nil, nonce, data, nil)
	// 		result := append(ciphertext, nonce...)

	// 		for i := len(nonce) - 1; i >= 0; i-- {
	// 			if nonce[i] < 255 {
	// 				nonce[i]++
	// 				break
	// 			}
	// 			nonce[i] = 0
	// 		}
	// 		return result
	// 	}

	// 	return keyId, writerCallback, nil
	// }

	// ReaderHook = func(id string, context []byte) (func(data []byte) ([]byte, error), error) {
	// 	if id != keyId {
	// 		return nil, fmt.Errorf("unknown callback ID: %s", id)
	// 	}

	// 	derivedKey := make([]byte, 32)
	// 	derivedKey, err := crypto.OpenSSLKBKDFDeriveKey(key, label, context, derivedKey, "SHA2-256", "")
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	block, err := aes.NewCipher(derivedKey)
	// 	if err != nil {
	// 		panic("Failed to create AES cipher: " + err.Error())
	// 	}

	// 	aesgcm, err := cipher.NewGCM(block)
	// 	if err != nil {
	// 		panic("Failed to create AES GCM: " + err.Error())
	// 	}

	// 	readerCallback := func(data []byte) ([]byte, error) {

	// 		if len(data) < 12 {
	// 			return nil, fmt.Errorf("ciphertext too short")
	// 		}

	// 		nonce := data[len(data)-12:]
	// 		ciphertext := data[:len(data)-12]
	// 		plaintext, err := aesgcm.Open(nil, nonce, ciphertext, nil)
	// 		if err != nil {
	// 			return nil, fmt.Errorf("failed to decrypt data: %w", err)
	// 		}

	// 		return plaintext, nil
	// 	}

	// 	return readerCallback, nil
	// }

	// zapv16.WriterHook = WriterHook
	// zapv16.ReaderHook = ReaderHook
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
