package util

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	mrand "math/rand"
	"testing"

	crypto "github.com/couchbase/gocbcrypto"
)

func genKey(b *testing.B) []byte {
	label := []byte("search")

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		b.Fatalf("Failed to generate random key: %v", err)
	}

	context := make([]byte, 8)
	if _, err := rand.Read(context); err != nil {
		b.Fatalf("Failed to generate random context: %v", err)
	}

	derivedKey := make([]byte, 32)
	derivedKey, err := crypto.OpenSSLKBKDFDeriveKey(key, label, context, derivedKey, "SHA2-256", "")
	if err != nil {
		b.Fatalf("Failed to derive key: %v", err)
	}

	return derivedKey
}

func genCipher(b *testing.B) cipher.AEAD {
	derivedKey := genKey(b)

	block, err := aes.NewCipher(derivedKey)
	if err != nil {
		b.Fatalf("Failed to create AES cipher: %v", err)
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		b.Fatalf("Failed to create AES GCM: %v", err)
	}

	return aesgcm
}

func genNonce(b *testing.B) []byte {
	nonce := make([]byte, 12)
	if _, err := rand.Read(nonce); err != nil {
		b.Fatalf("Failed to generate random nonce: %v", err)
	}
	return nonce
}

func BenchmarkGoCryptoWrite100B(b *testing.B) {
	aesgcm := genCipher(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 100)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		b.StartTimer()
		buf := make([]byte, aesgcm.Overhead()+len(data))
		_ = aesgcm.Seal(buf[:0], nonce, data, nil)
	}
}

func BenchmarkGoCryptoWrite1KB(b *testing.B) {
	aesgcm := genCipher(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		b.StartTimer()
		buf := make([]byte, aesgcm.Overhead()+len(data))
		_ = aesgcm.Seal(buf[:0], nonce, data, nil)
	}
}

func TestGoCryptoWrite1KB(t *testing.T) {
	label := []byte("search")

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("Failed to generate random key: %v", err)
	}

	context := make([]byte, 8)
	if _, err := rand.Read(context); err != nil {
		t.Fatalf("Failed to generate random context: %v", err)
	}

	derivedKey := make([]byte, 32)
	derivedKey, err := crypto.OpenSSLKBKDFDeriveKey(key, label, context, derivedKey, "SHA2-256", "")
	if err != nil {
		t.Fatalf("Failed to derive key: %v", err)
	}

	block, err := aes.NewCipher(derivedKey)
	if err != nil {
		t.Fatalf("Failed to create AES cipher: %v", err)
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("Failed to create AES GCM: %v", err)
	}

	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(mrand.Intn(256))
	}

	nonce := make([]byte, 12)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatalf("Failed to generate random nonce: %v", err)
	}

	buf := make([]byte, aesgcm.Overhead()+len(data))

	ciphertext := aesgcm.Seal(buf[:0], nonce, data, nil)
	plaintext := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(ciphertext), 16, false))
	_, err = crypto.OpenSSLAes256GCMDecrypt(derivedKey, nonce, ciphertext, plaintext, 16, "")
	if err != nil {
		t.Errorf("Failed to decrypt data: %v", err)
	}
}

func BenchmarkGoCryptoWrite10KB(b *testing.B) {
	aesgcm := genCipher(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 10240)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		b.StartTimer()
		buf := make([]byte, aesgcm.Overhead()+len(data))
		_ = aesgcm.Seal(buf[:0], nonce, data, nil)
	}
}

func BenchmarkGoCryptoWrite100KB(b *testing.B) {
	aesgcm := genCipher(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 102400)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		b.StartTimer()
		buf := make([]byte, aesgcm.Overhead()+len(data))
		_ = aesgcm.Seal(buf[:0], nonce, data, nil)
	}
}

func BenchmarkGoCryptoWrite1MB(b *testing.B) {
	aesgcm := genCipher(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024*1024)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		b.StartTimer()
		buf := make([]byte, aesgcm.Overhead()+len(data))
		_ = aesgcm.Seal(buf[:0], nonce, data, nil)
	}
}

func BenchmarkGoCryptoWrite10MB(b *testing.B) {
	aesgcm := genCipher(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024*1024*10)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		b.StartTimer()
		buf := make([]byte, aesgcm.Overhead()+len(data))
		_ = aesgcm.Seal(buf[:0], nonce, data, nil)
	}
}

func BenchmarkGoCryptoWrite100MB(b *testing.B) {
	aesgcm := genCipher(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024*1024*100)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		b.StartTimer()
		buf := make([]byte, aesgcm.Overhead()+len(data))
		_ = aesgcm.Seal(buf[:0], nonce, data, nil)
	}
}

func BenchmarkGoCryptoWrite1GB(b *testing.B) {
	aesgcm := genCipher(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024*1024*1024)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		b.StartTimer()
		buf := make([]byte, aesgcm.Overhead()+len(data))
		_ = aesgcm.Seal(buf[:0], nonce, data, nil)
	}
}

func BenchmarkGOCBCryptoWrite100B(b *testing.B) {
	key := genKey(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 100)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		b.StartTimer()
		out := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(data), 16, true))
		_, err := crypto.OpenSSLAes256GCMEncrypt(key, nonce, data, out, 16, "")
		if err != nil {
			b.Errorf("Failed to encrypt data: %v", err)
		}
	}
}

func BenchmarkGOCBCryptoWrite1KB(b *testing.B) {
	key := genKey(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		b.StartTimer()
		out := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(data), 16, true))
		_, err := crypto.OpenSSLAes256GCMEncrypt(key, nonce, data, out, 16, "")
		if err != nil {
			b.Errorf("Failed to encrypt data: %v", err)
		}
	}
}

func BenchmarkGOCBCryptoWrite10KB(b *testing.B) {
	key := genKey(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 10240)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		b.StartTimer()
		out := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(data), 16, true))
		_, err := crypto.OpenSSLAes256GCMEncrypt(key, nonce, data, out, 16, "")
		if err != nil {
			b.Errorf("Failed to encrypt data: %v", err)
		}
	}
}

func BenchmarkGOCBCryptoWrite100KB(b *testing.B) {
	key := genKey(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 102400)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		b.StartTimer()
		out := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(data), 16, true))
		_, err := crypto.OpenSSLAes256GCMEncrypt(key, nonce, data, out, 16, "")
		if err != nil {
			b.Errorf("Failed to encrypt data: %v", err)
		}
	}
}

func BenchmarkGOCBCryptoWrite1MB(b *testing.B) {
	key := genKey(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024*1024)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		b.StartTimer()
		out := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(data), 16, true))
		_, err := crypto.OpenSSLAes256GCMEncrypt(key, nonce, data, out, 16, "")
		if err != nil {
			b.Errorf("Failed to encrypt data: %v", err)
		}
	}
}

func BenchmarkGOCBCryptoWrite10MB(b *testing.B) {
	key := genKey(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024*1024*10)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		b.StartTimer()
		out := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(data), 16, true))
		_, err := crypto.OpenSSLAes256GCMEncrypt(key, nonce, data, out, 16, "")
		if err != nil {
			b.Errorf("Failed to encrypt data: %v", err)
		}
	}
}

func BenchmarkGOCBCryptoWrite100MB(b *testing.B) {
	key := genKey(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024*1024*100)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		b.StartTimer()
		out := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(data), 16, true))
		_, err := crypto.OpenSSLAes256GCMEncrypt(key, nonce, data, out, 16, "")
		if err != nil {
			b.Errorf("Failed to encrypt data: %v", err)
		}
	}
}

func BenchmarkGOCBCryptoWrite1GB(b *testing.B) {
	key := genKey(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024*1024*1024)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		b.StartTimer()
		out := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(data), 16, true))
		_, err := crypto.OpenSSLAes256GCMEncrypt(key, nonce, data, out, 16, "")
		if err != nil {
			b.Errorf("Failed to encrypt data: %v", err)
		}
	}
}

func BenchmarkGoCryptoRead100B(b *testing.B) {
	aesgcm := genCipher(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 100)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		ciphertext := aesgcm.Seal(nil, nonce, data, nil)
		b.StartTimer()

		buf := make([]byte, aesgcm.Overhead()+len(data))
		_, err := aesgcm.Open(buf[:0], nonce, ciphertext, nil)
		if err != nil {
			b.Errorf("Failed to decrypt data: %v", err)
		}
	}
}

func BenchmarkGoCryptoRead1KB(b *testing.B) {
	aesgcm := genCipher(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		ciphertext := aesgcm.Seal(nil, nonce, data, nil)
		b.StartTimer()

		buf := make([]byte, aesgcm.Overhead()+len(data))
		_, err := aesgcm.Open(buf[:0], nonce, ciphertext, nil)
		if err != nil {
			b.Errorf("Failed to decrypt data: %v", err)
		}
	}
}

func BenchmarkGoCryptoRead10KB(b *testing.B) {
	aesgcm := genCipher(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 10240)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		ciphertext := aesgcm.Seal(nil, nonce, data, nil)
		b.StartTimer()

		buf := make([]byte, aesgcm.Overhead()+len(data))
		_, err := aesgcm.Open(buf[:0], nonce, ciphertext, nil)
		if err != nil {
			b.Errorf("Failed to decrypt data: %v", err)
		}
	}
}

func BenchmarkGoCryptoRead100KB(b *testing.B) {
	aesgcm := genCipher(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024*100)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		ciphertext := aesgcm.Seal(nil, nonce, data, nil)
		b.StartTimer()

		buf := make([]byte, aesgcm.Overhead()+len(data))
		_, err := aesgcm.Open(buf[:0], nonce, ciphertext, nil)
		if err != nil {
			b.Errorf("Failed to decrypt data: %v", err)
		}
	}
}

func BenchmarkGoCryptoRead1MB(b *testing.B) {
	aesgcm := genCipher(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024*1024)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		ciphertext := aesgcm.Seal(nil, nonce, data, nil)
		b.StartTimer()

		buf := make([]byte, aesgcm.Overhead()+len(data))
		_, err := aesgcm.Open(buf[:0], nonce, ciphertext, nil)
		if err != nil {
			b.Errorf("Failed to decrypt data: %v", err)
		}
	}
}

func BenchmarkGoCryptoRead10MB(b *testing.B) {
	aesgcm := genCipher(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024*1024*10)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		ciphertext := aesgcm.Seal(nil, nonce, data, nil)
		b.StartTimer()

		buf := make([]byte, aesgcm.Overhead()+len(data))
		_, err := aesgcm.Open(buf[:0], nonce, ciphertext, nil)
		if err != nil {
			b.Errorf("Failed to decrypt data: %v", err)
		}
	}
}

func BenchmarkGoCryptoRead100MB(b *testing.B) {
	aesgcm := genCipher(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024*1024*100)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		ciphertext := aesgcm.Seal(nil, nonce, data, nil)
		b.StartTimer()

		buf := make([]byte, aesgcm.Overhead()+len(data))
		_, err := aesgcm.Open(buf[:0], nonce, ciphertext, nil)
		if err != nil {
			b.Errorf("Failed to decrypt data: %v", err)
		}
	}
}

func BenchmarkGoCryptoRead1GB(b *testing.B) {
	aesgcm := genCipher(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024*1024*1024)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		ciphertext := aesgcm.Seal(nil, nonce, data, nil)
		b.StartTimer()

		buf := make([]byte, aesgcm.Overhead()+len(data))
		_, err := aesgcm.Open(buf[:0], nonce, ciphertext, nil)
		if err != nil {
			b.Errorf("Failed to decrypt data: %v", err)
		}
	}
}

func BenchmarkGOCBCryptoRead100B(b *testing.B) {
	key := genKey(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 100)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		ciphertext := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(data), 16, true))
		_, err := crypto.OpenSSLAes256GCMEncrypt(key, nonce, data, ciphertext, 16, "")
		if err != nil {
			b.Errorf("Failed to encrypt data: %v", err)
		}

		b.StartTimer()
		plaintext := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(ciphertext), 16, false))
		_, err = crypto.OpenSSLAes256GCMDecrypt(key, nonce, ciphertext, plaintext, 16, "")
		if err != nil {
			b.Errorf("Failed to decrypt data: %v", err)
		}
	}
}

func BenchmarkGOCBCryptoRead1KB(b *testing.B) {
	key := genKey(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		ciphertext := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(data), 16, true))
		_, err := crypto.OpenSSLAes256GCMEncrypt(key, nonce, data, ciphertext, 16, "")
		if err != nil {
			b.Errorf("Failed to encrypt data: %v", err)
		}

		b.StartTimer()
		plaintext := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(ciphertext), 16, false))
		_, err = crypto.OpenSSLAes256GCMDecrypt(key, nonce, ciphertext, plaintext, 16, "")
		if err != nil {
			b.Errorf("Failed to decrypt data: %v", err)
		}
	}
}

func BenchmarkGOCBCryptoRead10KB(b *testing.B) {
	key := genKey(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024*10)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		ciphertext := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(data), 16, true))
		_, err := crypto.OpenSSLAes256GCMEncrypt(key, nonce, data, ciphertext, 16, "")
		if err != nil {
			b.Errorf("Failed to encrypt data: %v", err)
		}

		b.StartTimer()
		plaintext := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(ciphertext), 16, false))
		_, err = crypto.OpenSSLAes256GCMDecrypt(key, nonce, ciphertext, plaintext, 16, "")
		if err != nil {
			b.Errorf("Failed to decrypt data: %v", err)
		}
	}
}

func BenchmarkGOCBCryptoRead100KB(b *testing.B) {
	key := genKey(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024*100)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		ciphertext := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(data), 16, true))
		_, err := crypto.OpenSSLAes256GCMEncrypt(key, nonce, data, ciphertext, 16, "")
		if err != nil {
			b.Errorf("Failed to encrypt data: %v", err)
		}

		b.StartTimer()
		plaintext := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(ciphertext), 16, false))
		_, err = crypto.OpenSSLAes256GCMDecrypt(key, nonce, ciphertext, plaintext, 16, "")
		if err != nil {
			b.Errorf("Failed to decrypt data: %v", err)
		}
	}
}

func BenchmarkGOCBCryptoRead1MB(b *testing.B) {
	key := genKey(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024*1024)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		ciphertext := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(data), 16, true))
		_, err := crypto.OpenSSLAes256GCMEncrypt(key, nonce, data, ciphertext, 16, "")
		if err != nil {
			b.Errorf("Failed to encrypt data: %v", err)
		}

		b.StartTimer()
		plaintext := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(ciphertext), 16, false))
		_, err = crypto.OpenSSLAes256GCMDecrypt(key, nonce, ciphertext, plaintext, 16, "")
		if err != nil {
			b.Errorf("Failed to decrypt data: %v", err)
		}
	}
}

func BenchmarkGOCBCryptoRead10MB(b *testing.B) {
	key := genKey(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024*1024*10)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		ciphertext := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(data), 16, true))
		_, err := crypto.OpenSSLAes256GCMEncrypt(key, nonce, data, ciphertext, 16, "")
		if err != nil {
			b.Errorf("Failed to encrypt data: %v", err)
		}

		b.StartTimer()
		plaintext := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(ciphertext), 16, false))
		_, err = crypto.OpenSSLAes256GCMDecrypt(key, nonce, ciphertext, plaintext, 16, "")
		if err != nil {
			b.Errorf("Failed to decrypt data: %v", err)
		}
	}
}

func BenchmarkGOCBCryptoRead100MB(b *testing.B) {

	key := genKey(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024*1024*100)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		ciphertext := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(data), 16, true))
		_, err := crypto.OpenSSLAes256GCMEncrypt(key, nonce, data, ciphertext, 16, "")
		if err != nil {
			b.Errorf("Failed to encrypt data: %v", err)
		}

		b.StartTimer()
		plaintext := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(ciphertext), 16, false))
		_, err = crypto.OpenSSLAes256GCMDecrypt(key, nonce, ciphertext, plaintext, 16, "")
		if err != nil {
			b.Errorf("Failed to decrypt data: %v", err)
		}
	}
}

func BenchmarkGOCBCryptoRead1GB(b *testing.B) {

	key := genKey(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nonce := genNonce(b)

		data := make([]byte, 1024*1024*1024)
		for i := range data {
			data[i] = byte(mrand.Intn(256))
		}

		ciphertext := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(data), 16, true))
		_, err := crypto.OpenSSLAes256GCMEncrypt(key, nonce, data, ciphertext, 16, "")
		if err != nil {
			b.Errorf("Failed to encrypt data: %v", err)
		}

		b.StartTimer()
		plaintext := make([]byte, crypto.OpenSSLAes256GCMOutputSize(len(ciphertext), 16, false))
		_, err = crypto.OpenSSLAes256GCMDecrypt(key, nonce, ciphertext, plaintext, 16, "")
		if err != nil {
			b.Errorf("Failed to decrypt data: %v", err)
		}
	}
}
