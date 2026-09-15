package s3

import (
	"strings"
	"testing"
)

func TestValidBucketName(t *testing.T) {
	valid := []string{"abc", "my-bucket", "my.bucket", "a1b2c3", "e2e-a-3f9c-data", strings.Repeat("a", 63), "1bucket", "a-.b", "demo-dest"}
	invalid := []string{"", "ab", strings.Repeat("a", 64), "My-Bucket", "-bucket", "bucket-", "bucket.", ".bucket", "my..bucket",
		"my_bucket", "192.168.5.4", "xn--bucket", "sthree-b", "amzn-s3-demo-x", "b-s3alias", "b--ol-s3", "b.mrap", "b--x-s3", "b--table-s3", "bu cket", "bücket"}
	for _, n := range valid {
		if !ValidBucketName(n) {
			t.Errorf("ValidBucketName(%q) = false, want true", n)
		}
	}
	for _, n := range invalid {
		if ValidBucketName(n) {
			t.Errorf("ValidBucketName(%q) = true, want false", n)
		}
	}
}

func BenchmarkValidBucketName(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		if !ValidBucketName("e2e-a-3f9c-training-sets.2026") {
			b.Fatal("invalid")
		}
	}
}
