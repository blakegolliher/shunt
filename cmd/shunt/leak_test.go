package main

import "testing"

func TestLocationLeaksNamesMinIO(t *testing.T) {
	for _, typ := range []string{"minio", "aws", "vast", "s3"} {
		if locationLeaks[typ] == "" {
			t.Errorf("cluster type %s has no <Location> leak entry", typ)
		}
	}
}
