package migrate

import (
	"strings"
	"testing"
)

func TestUploadIDRoundTrip(t *testing.T) {
	for _, backend := range []string{
		"2~abc", "VXBsb2FkIElEIGZvciBlbHZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA", "a1b2c3~d4", "~", "x", "has spaces/and?query=1&", "0123456789abcdef0123456789abcdef",
	} {
		enc := encodeUploadID("0f3a9c", backend)
		id, got, ok := DecodeUploadID(enc)
		if !ok || id != "0f3a9c" || got != backend {
			t.Errorf("%q: encoded %q decoded (%q, %q, %v)", backend, enc, id, got, ok)
		}
	}
	if encodeUploadID("0f3a9c", "") != "" {
		t.Error("empty id was prefixed")
	}
}

func TestUnprefixedTolerance(t *testing.T) {
	for _, v := range []string{"", "plainid", "abc~def", "0F3A9C~upper", "0f3a9~short", "0f3a9cc~long", "zzzzzz~nothex", "~leading"} {
		id, backend, ok := DecodeUploadID(v)
		if ok || id != "" || backend != v {
			t.Errorf("%q: (%q, %q, %v), want unprefixed", v, id, backend, ok)
		}
	}
}

func FuzzDecodeUploadID(f *testing.F) {
	for _, s := range []string{"", "0f3a9c~abc", "abc", "0f3a9c~~", "~~~~~~~"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, v string) {
		id, backend, ok := DecodeUploadID(v)
		if !ok {
			if backend != v || id != "" {
				t.Fatalf("unprefixed %q altered: %q %q", v, id, backend)
			}
			return
		}
		if encodeUploadID(id, backend) != v && backend != "" {
			t.Fatalf("%q does not re-encode: %q %q", v, id, backend)
		}
		if len(id) != idLen || strings.Contains(id, "~") {
			t.Fatalf("bad id %q from %q", id, v)
		}
	})
}

func BenchmarkDecodeUploadID(b *testing.B) {
	v := encodeUploadID("0f3a9c", "VXBsb2FkIElEIGZvciBlbHZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA")
	b.ReportAllocs()
	for b.Loop() {
		if _, _, ok := DecodeUploadID(v); !ok {
			b.Fatal("not prefixed")
		}
	}
}
