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

package s3response

import (
	"encoding/xml"
	"errors"
	"io"
	"time"
)

// ErrInvalidDate is returned by AmzDate.UnmarshalXML for a date in none of the accepted formats.
var ErrInvalidDate = errors.New("s3response: invalid date")

const (
	iso8601TimeFormat         = "2006-01-02T15:04:05.000Z"
	iso8601TimeFormatExtended = "2006-01-02T15:04:05.000000Z"
	iso8601TimeFormatWithTZ   = "2006-01-02T15:04:05-0700"
)

type PutObjectOutput struct {
	ETag              string
	VersionID         string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	ChecksumCRC64NVME *string
	ChecksumSHA512    *string
	ChecksumMD5       *string
	Size              *int64
	ChecksumType      ChecksumType
}

// Part describes part metadata.
type Part struct {
	PartNumber        int
	LastModified      time.Time
	ETag              string
	Size              int64
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	ChecksumCRC64NVME *string
	ChecksumSHA512    *string
	ChecksumMD5       *string
}

func (p Part) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type Alias Part
	aux := &struct {
		LastModified string `xml:"LastModified"`
		*Alias
	}{
		Alias: (*Alias)(&p),
	}

	aux.LastModified = p.LastModified.UTC().Format(time.RFC3339)

	return e.EncodeElement(aux, start)
}

// ListPartsResponse - s3 api list parts response.
type ListPartsResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListPartsResult" json:"-"`

	Bucket            string
	Key               string
	UploadID          string `xml:"UploadId"`
	ChecksumAlgorithm ChecksumAlgorithm
	ChecksumType      ChecksumType

	Initiator Initiator
	Owner     Owner

	// The class of storage used to store the object.
	StorageClass StorageClass

	PartNumberMarker     int
	NextPartNumberMarker int
	MaxParts             int
	IsTruncated          bool

	// List of parts.
	Parts []Part `xml:"Part"`
}

type ObjectAttributes string

const (
	ObjectAttributesEtag         ObjectAttributes = "ETag"
	ObjectAttributesChecksum     ObjectAttributes = "Checksum"
	ObjectAttributesObjectParts  ObjectAttributes = "ObjectParts"
	ObjectAttributesStorageClass ObjectAttributes = "StorageClass"
	ObjectAttributesObjectSize   ObjectAttributes = "ObjectSize"
)

func (o ObjectAttributes) IsValid() bool {
	return o == ObjectAttributesChecksum ||
		o == ObjectAttributesEtag ||
		o == ObjectAttributesObjectParts ||
		o == ObjectAttributesObjectSize ||
		o == ObjectAttributesStorageClass
}

type GetObjectAttributesResponse struct {
	ETag         *string
	ObjectSize   *int64
	StorageClass StorageClass `xml:",omitempty"`
	ObjectParts  *ObjectParts
	Checksum     *Checksum

	// Not included in the response body
	VersionId    *string
	LastModified *time.Time
	DeleteMarker *bool
}

type ObjectParts struct {
	PartNumberMarker     int
	NextPartNumberMarker int
	MaxParts             int
	IsTruncated          bool
	Parts                []ObjectPart `xml:"Part"`
}

// ListMultipartUploadsResponse - s3 api list multipart uploads response.
type ListMultipartUploadsResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListMultipartUploadsResult" json:"-"`

	Bucket             string
	KeyMarker          string
	UploadIDMarker     string `xml:"UploadIdMarker"`
	NextKeyMarker      string
	NextUploadIDMarker string `xml:"NextUploadIdMarker"`
	Delimiter          string
	Prefix             string
	EncodingType       string `xml:"EncodingType,omitempty"`
	MaxUploads         int
	IsTruncated        bool

	// List of pending uploads.
	Uploads []Upload `xml:"Upload"`

	// Delimed common prefixes.
	CommonPrefixes []CommonPrefix
}

type ListObjectsResult struct {
	XMLName        xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult" json:"-"`
	Name           *string
	Prefix         *string
	Marker         *string
	NextMarker     *string
	MaxKeys        *int32
	Delimiter      *string
	IsTruncated    *bool
	Contents       []Object
	CommonPrefixes []CommonPrefix
	EncodingType   EncodingType
}

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

// Upload describes in progress multipart upload
type Upload struct {
	Key               string
	UploadID          string `xml:"UploadId"`
	Initiator         Initiator
	Owner             Owner
	StorageClass      StorageClass
	Initiated         time.Time
	ChecksumAlgorithm ChecksumAlgorithm
	ChecksumType      ChecksumType
}

func (u Upload) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type Alias Upload
	aux := &struct {
		Initiated string `xml:"Initiated"`
		*Alias
	}{
		Alias: (*Alias)(&u),
	}

	aux.Initiated = u.Initiated.UTC().Format(time.RFC3339)

	return e.EncodeElement(aux, start)
}

// CommonPrefix ListObjectsResponse common prefixes (directory abstraction)
type CommonPrefix struct {
	Prefix string
}

// Initiator same fields as Owner
type Initiator Owner

// Owner bucket ownership
type Owner struct {
	ID          string
	DisplayName string
}

type Tag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type TagSet struct {
	Tags []Tag `xml:"Tag"`
}

type Tagging struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Tagging" json:"-"`
	TagSet  TagSet   `xml:"TagSet"`
}

// UnmarshalXML accepts Tagging documents both with and without the S3 XML
// namespace, while xml.Marshal continues to emit the namespace via XMLName.
func (t *Tagging) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type plain struct {
		TagSet TagSet `xml:"TagSet"`
	}
	var p plain
	if err := d.DecodeElement(&p, &start); err != nil {
		return err
	}
	t.TagSet = p.TagSet
	return nil
}

type DeleteObjects struct {
	Objects []ObjectIdentifier `xml:"Object"`
}

type DeleteResult struct {
	Deleted []DeletedObject
	Error   []Error
}
type ListBucketsInput struct {
	Owner             string
	IsAdmin           bool
	ContinuationToken string
	Prefix            string
	MaxBuckets        int32
}

type ListAllMyBucketsResult struct {
	XMLName           xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult" json:"-"`
	Owner             CanonicalUser
	Buckets           ListAllMyBucketsList
	ContinuationToken string `xml:"ContinuationToken,omitempty"`
	Prefix            string `xml:"Prefix,omitempty"`
}

type ListAllMyBucketsEntry struct {
	Name         string
	BucketRegion string
	CreationDate time.Time
}

func (r ListAllMyBucketsEntry) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type Alias ListAllMyBucketsEntry
	aux := &struct {
		CreationDate string `xml:"CreationDate"`
		*Alias
	}{
		Alias: (*Alias)(&r),
	}

	aux.CreationDate = r.CreationDate.UTC().Format(time.RFC3339)

	return e.EncodeElement(aux, start)
}

type ListAllMyBucketsList struct {
	Bucket []ListAllMyBucketsEntry
}

type CanonicalUser struct {
	ID          string
	DisplayName string
}

type CopyObjectOutput struct {
	BucketKeyEnabled        *bool
	CopyObjectResult        *CopyObjectResult
	CopySourceVersionId     *string
	Expiration              *string
	SSECustomerAlgorithm    *string
	SSECustomerKeyMD5       *string
	SSEKMSEncryptionContext *string
	SSEKMSKeyId             *string
	ServerSideEncryption    ServerSideEncryption
	VersionId               *string
}

type CopyObjectResult struct {
	XMLName           xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopyObjectResult" json:"-"`
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	ChecksumSHA512    *string
	ChecksumMD5       *string
	ChecksumType      ChecksumType
	ETag              *string
	LastModified      *time.Time
}

func (r CopyObjectResult) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type Alias CopyObjectResult
	aux := &struct {
		LastModified string `xml:"LastModified,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(&r),
	}
	if r.LastModified != nil {
		aux.LastModified = r.LastModified.UTC().Format(time.RFC3339)
	}

	return e.EncodeElement(aux, start)
}

type CopyPartResult struct {
	XMLName           xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopyPartResult" json:"-"`
	LastModified      time.Time
	ETag              *string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	ChecksumCRC64NVME *string
	ChecksumSHA512    *string
	ChecksumMD5       *string

	// not included in the body
	CopySourceVersionId string `xml:"-"`
}

func (r CopyPartResult) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type Alias CopyPartResult
	aux := &struct {
		LastModified string `xml:"LastModified,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(&r),
	}
	if !r.LastModified.IsZero() {
		aux.LastModified = r.LastModified.UTC().Format(time.RFC3339)
	}

	return e.EncodeElement(aux, start)
}

type CompleteMultipartUploadRequestBody struct {
	XMLName xml.Name        `xml:"CompleteMultipartUpload" json:"-"`
	Parts   []CompletedPart `xml:"Part"`
}

type CompleteMultipartUploadResult struct {
	XMLName           xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUploadResult" json:"-"`
	Location          *string
	Bucket            *string
	Key               *string
	ETag              *string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	ChecksumCRC64NVME *string
	ChecksumSHA512    *string
	ChecksumMD5       *string
	ChecksumType      *ChecksumType
}

type AccessControlPolicy struct {
	XMLName           xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AccessControlPolicy" json:"-"`
	Owner             CanonicalUser
	AccessControlList AccessControlList
}

type AccessControlList struct {
	Grant []Grant
}

type Grant struct {
	Grantee    Grantee
	Permission string
}

// Set the following to encode correctly:
//
//	Grantee: s3response.Grantee{
//		Xsi:         "http://www.w3.org/2001/XMLSchema-instance",
//		Type:        "CanonicalUser",
//	},
type Grantee struct {
	XMLName     xml.Name `xml:"Grantee"`
	Xsi         string   `xml:"xmlns:xsi,attr,omitempty"`
	Type        string   `xml:"xsi:type,attr,omitempty"`
	ID          string
	DisplayName string
}

type OwnershipControls struct {
	Rules []OwnershipControlsRule `xml:"Rule"`
}

type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ InitiateMultipartUploadResult" json:"-"`
	Bucket   string
	Key      string
	UploadId string
}

type ListVersionsResult struct {
	XMLName             xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListVersionsResult" json:"-"`
	CommonPrefixes      []CommonPrefix
	DeleteMarkers       []DeleteMarkerEntry `xml:"DeleteMarker"`
	Delimiter           *string
	EncodingType        EncodingType
	IsTruncated         *bool
	KeyMarker           *string
	MaxKeys             *int32
	Name                *string
	NextKeyMarker       *string
	NextVersionIdMarker *string
	Prefix              *string
	VersionIdMarker     *string
	Versions            []ObjectVersion `xml:"Version"`
}

type ObjectVersion struct {
	ChecksumAlgorithm []ChecksumAlgorithm
	ChecksumType      ChecksumType
	ETag              *string
	IsLatest          *bool
	Key               *string
	LastModified      *time.Time
	Owner             *Owner
	RestoreStatus     *RestoreStatus
	Size              *int64
	StorageClass      ObjectVersionStorageClass
	VersionId         *string
}

func (o ObjectVersion) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type Alias ObjectVersion
	aux := &struct {
		LastModified string `xml:"LastModified"`
		*Alias
	}{
		Alias: (*Alias)(&o),
	}

	if o.LastModified != nil {
		aux.LastModified = o.LastModified.UTC().Format(time.RFC3339)
	}

	return e.EncodeElement(aux, start)
}

type GetBucketVersioningOutput struct {
	XMLName   xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ VersioningConfiguration" json:"-"`
	MFADelete *MFADeleteStatus
	Status    *BucketVersioningStatus
}

type PutObjectRetentionInput struct {
	XMLName         xml.Name `xml:"Retention"`
	Mode            ObjectLockRetentionMode
	RetainUntilDate AmzDate
}

type PutObjectInput struct {
	ContentLength             *int64
	ObjectLockRetainUntilDate *time.Time

	Bucket                  *string
	Key                     *string
	ContentType             *string
	ContentEncoding         *string
	ContentDisposition      *string
	ContentLanguage         *string
	CacheControl            *string
	Expires                 *string
	Tagging                 *string
	ChecksumCRC32           *string
	ChecksumCRC32C          *string
	ChecksumSHA1            *string
	ChecksumSHA256          *string
	ChecksumCRC64NVME       *string
	ChecksumSHA512          *string
	ChecksumMD5             *string
	ContentMD5              *string
	ExpectedBucketOwner     *string
	GrantFullControl        *string
	GrantRead               *string
	GrantReadACP            *string
	GrantWriteACP           *string
	IfMatch                 *string
	IfNoneMatch             *string
	SSECustomerAlgorithm    *string
	SSECustomerKey          *string
	SSECustomerKeyMD5       *string
	SSEKMSEncryptionContext *string
	SSEKMSKeyId             *string
	WebsiteRedirectLocation *string

	ObjectLockMode            ObjectLockMode
	ObjectLockLegalHoldStatus ObjectLockLegalHoldStatus
	ChecksumAlgorithm         ChecksumAlgorithm
	StorageClass              StorageClass

	Metadata map[string]string
	Body     io.Reader
}

type CreateMultipartUploadInput struct {
	Bucket                    *string
	Key                       *string
	ExpectedBucketOwner       *string
	CacheControl              *string
	ContentDisposition        *string
	ContentEncoding           *string
	ContentLanguage           *string
	ContentType               *string
	Expires                   *string
	SSECustomerAlgorithm      *string
	SSECustomerKey            *string
	SSECustomerKeyMD5         *string
	SSEKMSEncryptionContext   *string
	SSEKMSKeyId               *string
	GrantFullControl          *string
	GrantRead                 *string
	GrantReadACP              *string
	GrantWriteACP             *string
	Tagging                   *string
	WebsiteRedirectLocation   *string
	BucketKeyEnabled          *bool
	ObjectLockRetainUntilDate *time.Time
	Metadata                  map[string]string

	ACL                       ObjectCannedACL
	ChecksumAlgorithm         ChecksumAlgorithm
	ChecksumType              ChecksumType
	ObjectLockLegalHoldStatus ObjectLockLegalHoldStatus
	ObjectLockMode            ObjectLockMode
	RequestPayer              RequestPayer
	ServerSideEncryption      ServerSideEncryption
	StorageClass              StorageClass
}

type CopyObjectInput struct {
	Metadata                       map[string]string
	Bucket                         *string
	CopySource                     *string
	Key                            *string
	CacheControl                   *string
	ContentDisposition             *string
	ContentEncoding                *string
	ContentLanguage                *string
	ContentType                    *string
	CopySourceIfMatch              *string
	CopySourceIfNoneMatch          *string
	CopySourceSSECustomerAlgorithm *string
	CopySourceSSECustomerKey       *string
	CopySourceSSECustomerKeyMD5    *string
	ExpectedBucketOwner            *string
	ExpectedSourceBucketOwner      *string
	Expires                        *string
	GrantFullControl               *string
	GrantRead                      *string
	GrantReadACP                   *string
	GrantWriteACP                  *string
	SSECustomerAlgorithm           *string
	SSECustomerKey                 *string
	SSECustomerKeyMD5              *string
	SSEKMSEncryptionContext        *string
	SSEKMSKeyId                    *string
	Tagging                        *string
	WebsiteRedirectLocation        *string

	CopySourceIfModifiedSince   *time.Time
	CopySourceIfUnmodifiedSince *time.Time
	ObjectLockRetainUntilDate   *time.Time

	BucketKeyEnabled *bool

	ACL                       ObjectCannedACL
	ChecksumAlgorithm         ChecksumAlgorithm
	MetadataDirective         MetadataDirective
	ObjectLockLegalHoldStatus ObjectLockLegalHoldStatus
	ObjectLockMode            ObjectLockMode
	RequestPayer              RequestPayer
	ServerSideEncryption      ServerSideEncryption
	StorageClass              StorageClass
	TaggingDirective          TaggingDirective
}

type GetObjectLegalHoldResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LegalHold"`
	Status  ObjectLockLegalHoldStatus
}

type AmzDate struct {
	time.Time
}

// Parses the date from xml string and validates for predefined date formats
func (d *AmzDate) UnmarshalXML(e *xml.Decoder, startElement xml.StartElement) error {
	var dateStr string
	err := e.DecodeElement(&dateStr, &startElement)
	if err != nil {
		return err
	}

	retDate, err := d.ISO8601Parse(dateStr)
	if err != nil {
		return ErrInvalidDate
	}

	*d = AmzDate{retDate}
	return nil
}

// Encodes expiration date if it is non-zero
// Encodes empty string if it's zero
func (d AmzDate) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if d.IsZero() {
		return nil
	}
	return e.EncodeElement(d.UTC().Format(iso8601TimeFormat), startElement)
}

// Parses ISO8601 date string to time.Time by
// validating different time layouts
func (AmzDate) ISO8601Parse(date string) (t time.Time, err error) {
	for _, layout := range []string{
		iso8601TimeFormat,
		iso8601TimeFormatExtended,
		iso8601TimeFormatWithTZ,
		time.RFC3339,
	} {
		t, err = time.Parse(layout, date)
		if err == nil {
			return t, nil
		}
	}

	return t, err
}

type ChecksumSummary struct {
	Algorithm ChecksumAlgorithm
	Type      ChecksumType

	CRC32     *string
	CRC32C    *string
	SHA1      *string
	SHA256    *string
	CRC64NVME *string
	SHA512    *string
	MD5       *string
}

// LocationConstraint represents the GetBucketLocation response
type LocationConstraint struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LocationConstraint"`
	Value   *string  `xml:",chardata"`
}

type CreateBucketConfiguration struct {
	LocationConstraint *string
	TagSet             []Tag `xml:"Tags>Tag"`
}

type PostResponse struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ PostResponse"`
	Location string
	Bucket   string
	Key      string
	ETag     string
}
