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
	for _, s := range []string{"", "0f3a9c~abc", "abc", "0f3a9c~~", "~~~~~~~", "0f3a9c.a1b2c3~abc", "0f3a9c.a1b2c~abc"} {
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
		// The prefix is a cluster id, or a cluster id and a bucket tag (ADR-0018 N3b), never a '~'.
		cluster, tag := SplitUploadPrefix(id)
		if len(cluster) != idLen || !isLowerHex(cluster) || strings.Contains(id, "~") || (tag != "" && (len(tag) != idLen || !isLowerHex(tag))) {
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

// A tagged id names one of two buckets on a cluster; it decodes to its prefix whole, and the prefix
// splits into the cluster id and the tag. Malformed tags are not prefixes (ADR-0018 N3b).
func TestTaggedUploadID(t *testing.T) {
	tag := BucketTag("mimir-final")
	if len(tag) != 6 || !isLowerHex(tag) || tag == BucketTag("mimir-dest") {
		t.Fatalf("tags: %q %q", tag, BucketTag("mimir-dest"))
	}
	prefix, backend, ok := DecodeUploadID("0f3a9c." + tag + "~abc~def")
	if !ok || prefix != "0f3a9c."+tag || backend != "abc~def" {
		t.Fatalf("tagged: %q %q %v", prefix, backend, ok)
	}
	if id, got := SplitUploadPrefix(prefix); id != "0f3a9c" || got != tag {
		t.Fatalf("split: %q %q", id, got)
	}
	if id, got := SplitUploadPrefix("0f3a9c"); id != "0f3a9c" || got != "" {
		t.Fatalf("untagged split: %q %q", id, got)
	}
	for _, v := range []string{"0f3a9c.12345~x", "0f3a9c.1234567~x", "0f3a9c.ABCDEF~x", "0f3a9c-abcdef~x", "0f3a9cabcdef0~x"} {
		if _, backend, ok := DecodeUploadID(v); ok || backend != v {
			t.Errorf("%q: decoded as prefixed", v)
		}
	}
}
