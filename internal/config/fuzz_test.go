package config

import (
	"os"
	"path/filepath"
	"testing"
)

// FuzzParse: Parse must never panic, and any config it accepts must re-validate.
func FuzzParse(f *testing.F) {
	for _, glob := range []string{"testdata/valid/*.yaml", "testdata/invalid/*.yaml"} {
		files, _ := filepath.Glob(glob)
		for _, p := range files {
			b, err := os.ReadFile(p)
			if err != nil {
				f.Fatal(err)
			}
			f.Add(b)
		}
	}
	f.Add([]byte(""))
	f.Add([]byte("clusters: [1, 2]"))
	f.Add([]byte("clusters: {a: {endpoints: x}}"))
	f.Fuzz(func(t *testing.T, data []byte) {
		c, err := Parse(data)
		if err != nil {
			return
		}
		if err := c.Validate(); err != nil {
			t.Fatalf("accepted config fails re-validation: %v", err)
		}
	})
}
