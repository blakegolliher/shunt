// Copyright 2023 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
// Modified by Blake Golliher for github.com/blakegolliher/shunt, 2026-09-14: replaced
// aws-sdk-go-v2 service/s3/types with local types (go, same field names, same XML output);
// removed the s3err and debuglogger imports (AmzDate.UnmarshalXML returns ErrInvalidDate);
// removed SelectObjectContent structs (S3 Select is a shunt non-goal) and the admin-API
// Bucket/ListBucketsResult types; renamed the Checksum struct to ChecksumSummary to free the
// name for the S3 Checksum type; dropped the xxhash checksum fields (Versity extensions).
// Modified by Blake Golliher for github.com/blakegolliher/shunt, 2026-09-16: removed every type
// shunt does not use (G1 simplicity review); what remains is ListObjectsV2Result, Object, and the
// types they reference, which the merged listing (internal/proxy/merge.go) encodes and decodes.

package s3response

import (
	"encoding/xml"
	"time"
)

type ListObjectsV2Result struct {
	XMLName               xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult" json:"-"`
	Name                  *string
	Prefix                *string
	StartAfter            *string
	ContinuationToken     *string `xml:"ContinuationToken,omitempty"`
	NextContinuationToken *string
	KeyCount              *int32
	MaxKeys               *int32
	Delimiter             *string
	IsTruncated           *bool
	Contents              []Object
	CommonPrefixes        []CommonPrefix
	EncodingType          EncodingType
}

type Object struct {
	ChecksumAlgorithm []ChecksumAlgorithm
	ChecksumType      ChecksumType
	ETag              *string
	Key               *string
	LastModified      *time.Time
	Owner             *Owner
	RestoreStatus     *RestoreStatus
	Size              *int64
	StorageClass      ObjectStorageClass
}

func (o Object) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type Alias Object
	aux := &struct {
		LastModified string `xml:"LastModified,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(&o),
	}

	if o.LastModified != nil {
		aux.LastModified = o.LastModified.UTC().Format(time.RFC3339)
	}

	return e.EncodeElement(aux, start)
}

// CommonPrefix ListObjectsResponse common prefixes (directory abstraction)
type CommonPrefix struct {
	Prefix string
}

// Owner bucket ownership
type Owner struct {
	ID          string
	DisplayName string
}
