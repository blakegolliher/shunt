package cp

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestCipherRoundTrip(t *testing.T) {
	dir := t.TempDir()
	key, err := LoadOrCreateKey(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	again, err := LoadOrCreateKey(dir, false)
	if err != nil || string(again) != string(key) {
		t.Fatalf("the key did not persist: %v", err)
	}
	if st, _ := os.Stat(filepath.Join(dir, keyFile)); st.Mode().Perm() != 0o600 {
		t.Errorf("key file mode %04o, want 0600", st.Mode().Perm())
	}
	c, err := NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := c.Encrypt("cluster-secret")
	if err != nil {
		t.Fatal(err)
	}
	if sealed == "cluster-secret" || len(sealed) < 20 {
		t.Fatalf("not sealed: %q", sealed)
	}
	if got, err := c.Decrypt(sealed); err != nil || got != "cluster-secret" {
		t.Fatalf("decrypt: %q %v", got, err)
	}
	other, _ := NewCipher(make([]byte, 32))
	if _, err := other.Decrypt(sealed); err == nil {
		t.Error("another key decrypted the secret")
	}
	for _, bad := range []string{"", "v1:", "v1:!!!", "v0:abc", "cluster-secret"} {
		if _, err := c.Decrypt(bad); err == nil {
			t.Errorf("decrypted %q", bad)
		}
	}
	if _, err := LoadOrCreateKey(t.TempDir(), false); err == nil {
		t.Error("a missing key was created without create")
	}
	// A join resumed after saving its bootstrap writes the same key again: accepted. Another key is
	// refused, and the file keeps the first.
	if err := WriteKey(dir, key); err != nil {
		t.Errorf("writing the same key again: %v", err)
	}
	if err := WriteKey(dir, bytes.Repeat([]byte{9}, 32)); err == nil {
		t.Error("overwrote an existing key with another")
	}
	if got, err := LoadOrCreateKey(dir, false); err != nil || !bytes.Equal(got, key) {
		t.Errorf("the key after a refused overwrite: %x %v", got, err)
	}
}
