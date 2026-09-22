package cp

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Secrets at rest (ADR-0015, §12.8): cluster secret keys and client secrets are stored in etcd
// encrypted with AES-256-GCM under one data-encryption key that only control nodes hold. The key
// is created by `shunt-control init` in the data directory and copied to a joining node over the
// join call. A member proxy never sees the key; it receives secrets in the clear over the control
// channel (TLS for which is deferred).

// keyFile is the data-encryption key's file in a control node's data directory: 32 bytes, hex.
const keyFile = "encryption.key"

// Cipher encrypts and decrypts secrets with the node's data-encryption key.
type Cipher struct {
	aead cipher.AEAD
	key  []byte
}

// NewCipher builds a Cipher from a 32-byte key.
func NewCipher(key []byte) (*Cipher, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("data-encryption key: want 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Cipher{aead: aead, key: append([]byte(nil), key...)}, nil
}

// Key returns the raw key, for handing to a joining node.
func (c *Cipher) Key() []byte { return append([]byte(nil), c.key...) }

// Derive returns a key for another purpose from the data-encryption key: sha256(key ‖ purpose).
// The confirmation tokens of dry runs are keyed this way (ADR-0017), so every control node
// verifies a token any node issued.
func (c *Cipher) Derive(purpose string) []byte {
	h := sha256.New()
	h.Write(c.key)
	h.Write([]byte(purpose))
	return h.Sum(nil)
}

// Encrypt returns "v1:" + base64(nonce ‖ ciphertext).
func (c *Cipher) Encrypt(plaintext string) (string, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := c.aead.Seal(nonce, nonce, []byte(plaintext), nil)
	return "v1:" + base64.StdEncoding.EncodeToString(sealed), nil
}

// ErrCiphertext is returned for a value that is not this Cipher's output, or was sealed with
// another key.
var ErrCiphertext = errors.New("cannot decrypt a stored secret: not sealed with this control plane's data-encryption key")

// Decrypt reverses Encrypt.
func (c *Cipher) Decrypt(sealed string) (string, error) {
	body, ok := strings.CutPrefix(sealed, "v1:")
	if !ok {
		return "", ErrCiphertext
	}
	raw, err := base64.StdEncoding.DecodeString(body)
	if err != nil || len(raw) < c.aead.NonceSize() {
		return "", ErrCiphertext
	}
	n := c.aead.NonceSize()
	plain, err := c.aead.Open(nil, raw[:n], raw[n:], nil)
	if err != nil {
		return "", ErrCiphertext
	}
	return string(plain), nil
}

// LoadOrCreateKey reads the data-encryption key from dataDir, creating a fresh one (0600) when
// none exists and create is set. A joining node writes the key it received with WriteKey first.
func LoadOrCreateKey(dataDir string, create bool) ([]byte, error) {
	path := filepath.Join(dataDir, keyFile)
	data, err := os.ReadFile(path) //nolint:gosec // the node's own data directory
	switch {
	case err == nil:
		key, derr := hex.DecodeString(strings.TrimSpace(string(data)))
		if derr != nil || len(key) != 32 {
			return nil, fmt.Errorf("%s: want 64 hex characters (32 bytes)", path)
		}
		return key, nil
	case !errors.Is(err, fs.ErrNotExist):
		return nil, err
	case !create:
		return nil, fmt.Errorf("%s: no data-encryption key; this data directory was not initialized by `shunt-control init` or `join`", path)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	if err := WriteKey(dataDir, key); err != nil {
		return nil, err
	}
	return key, nil
}

// WriteKey stores the data-encryption key in dataDir, 0600, refusing to overwrite one.
func WriteKey(dataDir string, key []byte) error {
	if len(key) != 32 {
		return fmt.Errorf("data-encryption key: want 32 bytes, got %d", len(key))
	}
	path := filepath.Join(dataDir, keyFile)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // the node's own data directory
	if err != nil {
		return err
	}
	if _, err := f.WriteString(hex.EncodeToString(key) + "\n"); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
