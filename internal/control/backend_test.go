package control

import (
	"context"
	"testing"
)

// createBucket reports whether it made the bucket, so a caller undoing its work never deletes one
// it found: an owned bucket counts as created, but not as made by this call.
func TestCreateBucketReportsWhetherItMadeIt(t *testing.T) {
	rg := newRig(t)
	rg.vast01.owned = true
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	b, err := rg.ctl.backendFor("vast01")
	if err != nil {
		t.Fatal(err)
	}
	if created, err := b.createBucket(context.Background(), "fresh"); err != nil || !created {
		t.Fatalf("first create: created %v, %v", created, err)
	}
	if created, err := b.createBucket(context.Background(), "fresh"); err != nil || created {
		t.Fatalf("second create of an owned bucket: created %v, %v", created, err)
	}
}
