package config

import (
	"os"
	"testing"
)

func BenchmarkParse(b *testing.B) {
	for _, f := range []string{"poc", "mixed"} {
		data, err := os.ReadFile("testdata/valid/" + f + ".yaml")
		if err != nil {
			b.Fatal(err)
		}
		b.Run(f, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Parse(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkValidate(b *testing.B) {
	c, err := Load("testdata/valid/mixed.yaml")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := c.Validate(); err != nil {
			b.Fatal(err)
		}
	}
}
