package main

import "testing"

func TestSortBuckets(t *testing.T) {
	a := `<ListAllMyBucketsResult><Buckets><Bucket><CreationDate>*</CreationDate><Name>z</Name></Bucket><Bucket><CreationDate>*</CreationDate><Name>a</Name></Bucket></Buckets><Owner/></ListAllMyBucketsResult>`
	b := `<ListAllMyBucketsResult><Buckets><Bucket><CreationDate>*</CreationDate><Name>a</Name></Bucket><Bucket><CreationDate>*</CreationDate><Name>z</Name></Bucket></Buckets><Owner/></ListAllMyBucketsResult>`
	if sortBuckets(a) != sortBuckets(b) {
		t.Fatal("order-insensitive compare failed")
	}
	if sortBuckets("<Error/>") != "<Error/>" {
		t.Fatal("non-listing body changed")
	}
}
