package s3

import (
	"bytes"
	"encoding/xml"
	"net/http"
)

// Code is an S3 error code as it appears in the <Code> element of an error response.
type Code string

// S3 error codes shunt can originate itself. Codes from upstream responses are passed through
// untouched; this table is only consulted when shunt is the one answering the client.
// Wire values (code, HTTP status, message) are AWS's, from the S3 API "Error responses" reference.
const (
	AccessDenied                      Code = "AccessDenied"
	BadDigest                         Code = "BadDigest"
	BucketAlreadyExists               Code = "BucketAlreadyExists"
	BucketAlreadyOwnedByYou           Code = "BucketAlreadyOwnedByYou"
	BucketNotEmpty                    Code = "BucketNotEmpty"
	EntityTooLarge                    Code = "EntityTooLarge"
	EntityTooSmall                    Code = "EntityTooSmall"
	IncompleteBody                    Code = "IncompleteBody"
	InternalError                     Code = "InternalError"
	InvalidAccessKeyId                Code = "InvalidAccessKeyId" //nolint:revive // S3 wire name
	InvalidArgument                   Code = "InvalidArgument"
	InvalidBucketName                 Code = "InvalidBucketName"
	InvalidBucketState                Code = "InvalidBucketState"
	InvalidChunkSizeError             Code = "InvalidChunkSizeError"
	InvalidDigest                     Code = "InvalidDigest"
	InvalidLocationConstraint         Code = "InvalidLocationConstraint"
	InvalidObjectState                Code = "InvalidObjectState"
	InvalidPart                       Code = "InvalidPart"
	InvalidPartNumber                 Code = "InvalidPartNumber"
	InvalidPartOrder                  Code = "InvalidPartOrder"
	InvalidRange                      Code = "InvalidRange"
	InvalidRequest                    Code = "InvalidRequest"
	InvalidSecurity                   Code = "InvalidSecurity"
	InvalidToken                      Code = "InvalidToken"
	InvalidURI                        Code = "InvalidURI"
	KeyTooLongError                   Code = "KeyTooLongError"
	MalformedACLError                 Code = "MalformedACLError"
	MalformedPOSTRequest              Code = "MalformedPOSTRequest"
	MalformedTrailerError             Code = "MalformedTrailerError"
	MalformedXML                      Code = "MalformedXML"
	MaxMessageLengthExceeded          Code = "MaxMessageLengthExceeded"
	MetadataTooLarge                  Code = "MetadataTooLarge"
	MethodNotAllowed                  Code = "MethodNotAllowed"
	MissingContentLength              Code = "MissingContentLength"
	MissingRequestBodyError           Code = "MissingRequestBodyError"
	MissingSecurityHeader             Code = "MissingSecurityHeader"
	NoSuchBucket                      Code = "NoSuchBucket"
	NoSuchBucketPolicy                Code = "NoSuchBucketPolicy"
	NoSuchCORSConfiguration           Code = "NoSuchCORSConfiguration"
	NoSuchKey                         Code = "NoSuchKey"
	NoSuchLifecycleConfiguration      Code = "NoSuchLifecycleConfiguration"
	NoSuchTagSet                      Code = "NoSuchTagSet"
	NoSuchUpload                      Code = "NoSuchUpload"
	NoSuchVersion                     Code = "NoSuchVersion"
	NotImplemented                    Code = "NotImplemented"
	NotModified                       Code = "NotModified"
	OperationAborted                  Code = "OperationAborted"
	PermanentRedirect                 Code = "PermanentRedirect"
	PreconditionFailed                Code = "PreconditionFailed"
	QuotaExceeded                     Code = "QuotaExceeded"
	RequestHeaderSectionTooLarge      Code = "RequestHeaderSectionTooLarge"
	RequestTimeout                    Code = "RequestTimeout"
	RequestTimeTooSkewed              Code = "RequestTimeTooSkewed"
	RestoreAlreadyInProgress          Code = "RestoreAlreadyInProgress"
	ServiceUnavailable                Code = "ServiceUnavailable"
	SignatureDoesNotMatch             Code = "SignatureDoesNotMatch"
	SlowDown                          Code = "SlowDown"
	UnexpectedContent                 Code = "UnexpectedContent"
	XAmzContentSHA256Mismatch         Code = "XAmzContentSHA256Mismatch"
	AuthorizationHeaderMalformed      Code = "AuthorizationHeaderMalformed"
	AuthorizationQueryParametersError Code = "AuthorizationQueryParametersError"
	ExpiredToken                      Code = "ExpiredToken"
	InsufficientStorage               Code = "InsufficientStorage"
)

// Error is one row of the table: the wire code, its HTTP status, and AWS's message.
type Error struct {
	Code    Code
	Status  int
	Message string
}

func (e Error) Error() string { return string(e.Code) + ": " + e.Message }

// StatusCode implements the conventional accessor for handlers.
func (e Error) StatusCode() int { return e.Status }

var table = map[Code]Error{
	AccessDenied:                      {AccessDenied, http.StatusForbidden, "Access Denied"},
	BadDigest:                         {BadDigest, http.StatusBadRequest, "The Content-MD5 you specified did not match what we received."},
	BucketAlreadyExists:               {BucketAlreadyExists, http.StatusConflict, "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again."},
	BucketAlreadyOwnedByYou:           {BucketAlreadyOwnedByYou, http.StatusConflict, "Your previous request to create the named bucket succeeded and you already own it."},
	BucketNotEmpty:                    {BucketNotEmpty, http.StatusConflict, "The bucket you tried to delete is not empty."},
	EntityTooLarge:                    {EntityTooLarge, http.StatusBadRequest, "Your proposed upload exceeds the maximum allowed object size."},
	EntityTooSmall:                    {EntityTooSmall, http.StatusBadRequest, "Your proposed upload is smaller than the minimum allowed object size."},
	IncompleteBody:                    {IncompleteBody, http.StatusBadRequest, "You did not provide the number of bytes specified by the Content-Length HTTP header."},
	InternalError:                     {InternalError, http.StatusInternalServerError, "We encountered an internal error. Please try again."},
	InvalidAccessKeyId:                {InvalidAccessKeyId, http.StatusForbidden, "The AWS access key ID that you provided does not exist in our records."},
	InvalidArgument:                   {InvalidArgument, http.StatusBadRequest, "Invalid Argument"},
	InvalidBucketName:                 {InvalidBucketName, http.StatusBadRequest, "The specified bucket is not valid."},
	InvalidBucketState:                {InvalidBucketState, http.StatusConflict, "The request is not valid for the current state of the bucket."},
	InvalidChunkSizeError:             {InvalidChunkSizeError, http.StatusBadRequest, "Only the last chunk is allowed to have a size less than 8192 bytes"},
	InvalidDigest:                     {InvalidDigest, http.StatusBadRequest, "The Content-MD5 you specified is not valid."},
	InvalidLocationConstraint:         {InvalidLocationConstraint, http.StatusBadRequest, "The specified location constraint is not valid."},
	InvalidObjectState:                {InvalidObjectState, http.StatusForbidden, "The operation is not valid for the current state of the object."},
	InvalidPart:                       {InvalidPart, http.StatusBadRequest, "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag."},
	InvalidPartNumber:                 {InvalidPartNumber, http.StatusRequestedRangeNotSatisfiable, "The requested partnumber is not satisfiable"},
	InvalidPartOrder:                  {InvalidPartOrder, http.StatusBadRequest, "The list of parts was not in ascending order. The parts list must be specified in order by part number."},
	InvalidRange:                      {InvalidRange, http.StatusRequestedRangeNotSatisfiable, "The requested range cannot be satisfied."},
	InvalidRequest:                    {InvalidRequest, http.StatusBadRequest, "Invalid Request"},
	InvalidSecurity:                   {InvalidSecurity, http.StatusForbidden, "The provided security credentials are not valid."},
	InvalidToken:                      {InvalidToken, http.StatusBadRequest, "The provided token is malformed or otherwise invalid."},
	InvalidURI:                        {InvalidURI, http.StatusBadRequest, "Couldn't parse the specified URI."},
	KeyTooLongError:                   {KeyTooLongError, http.StatusBadRequest, "Your key is too long."},
	MalformedACLError:                 {MalformedACLError, http.StatusBadRequest, "The XML you provided was not well-formed or did not validate against our published schema."},
	MalformedPOSTRequest:              {MalformedPOSTRequest, http.StatusBadRequest, "The body of your POST request is not well-formed multipart/form-data."},
	MalformedTrailerError:             {MalformedTrailerError, http.StatusBadRequest, "The request contained trailing data that was not well-formed or did not conform to our published schema."},
	MalformedXML:                      {MalformedXML, http.StatusBadRequest, "The XML you provided was not well-formed or did not validate against our published schema."},
	MaxMessageLengthExceeded:          {MaxMessageLengthExceeded, http.StatusBadRequest, "Your request was too big."},
	MetadataTooLarge:                  {MetadataTooLarge, http.StatusBadRequest, "Your metadata headers exceed the maximum allowed metadata size."},
	MethodNotAllowed:                  {MethodNotAllowed, http.StatusMethodNotAllowed, "The specified method is not allowed against this resource."},
	MissingContentLength:              {MissingContentLength, http.StatusLengthRequired, "You must provide the Content-Length HTTP header."},
	MissingRequestBodyError:           {MissingRequestBodyError, http.StatusBadRequest, "Request body is empty."},
	MissingSecurityHeader:             {MissingSecurityHeader, http.StatusBadRequest, "Your request is missing a required header."},
	NoSuchBucket:                      {NoSuchBucket, http.StatusNotFound, "The specified bucket does not exist."},
	NoSuchBucketPolicy:                {NoSuchBucketPolicy, http.StatusNotFound, "The specified bucket does not have a bucket policy."},
	NoSuchCORSConfiguration:           {NoSuchCORSConfiguration, http.StatusNotFound, "The CORS configuration does not exist"},
	NoSuchKey:                         {NoSuchKey, http.StatusNotFound, "The specified key does not exist."},
	NoSuchLifecycleConfiguration:      {NoSuchLifecycleConfiguration, http.StatusNotFound, "The lifecycle configuration does not exist."},
	NoSuchTagSet:                      {NoSuchTagSet, http.StatusNotFound, "The TagSet does not exist."},
	NoSuchUpload:                      {NoSuchUpload, http.StatusNotFound, "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."},
	NoSuchVersion:                     {NoSuchVersion, http.StatusNotFound, "The version ID specified in the request does not match an existing version."},
	NotImplemented:                    {NotImplemented, http.StatusNotImplemented, "A header you provided implies functionality that is not implemented."},
	NotModified:                       {NotModified, http.StatusNotModified, "Not Modified"},
	OperationAborted:                  {OperationAborted, http.StatusConflict, "A conflicting conditional operation is currently in progress against this resource. Try again."},
	PermanentRedirect:                 {PermanentRedirect, http.StatusMovedPermanently, "The bucket you are attempting to access must be addressed using the specified endpoint. Send all future requests to this endpoint."},
	PreconditionFailed:                {PreconditionFailed, http.StatusPreconditionFailed, "At least one of the preconditions you specified did not hold."},
	QuotaExceeded:                     {QuotaExceeded, http.StatusForbidden, "Your request was denied due to quota exceeded."},
	RequestHeaderSectionTooLarge:      {RequestHeaderSectionTooLarge, http.StatusBadRequest, "Your request header section exceeds the maximum allowed size."},
	RequestTimeout:                    {RequestTimeout, http.StatusBadRequest, "Your socket connection to the server was not read from or written to within the timeout period."},
	RequestTimeTooSkewed:              {RequestTimeTooSkewed, http.StatusForbidden, "The difference between the request time and the server's time is too large."},
	RestoreAlreadyInProgress:          {RestoreAlreadyInProgress, http.StatusConflict, "Object restore is already in progress."},
	ServiceUnavailable:                {ServiceUnavailable, http.StatusServiceUnavailable, "Service is unable to handle request."},
	SignatureDoesNotMatch:             {SignatureDoesNotMatch, http.StatusForbidden, "The request signature we calculated does not match the signature you provided. Check your key and signing method."},
	SlowDown:                          {SlowDown, http.StatusServiceUnavailable, "Please reduce your request rate."},
	UnexpectedContent:                 {UnexpectedContent, http.StatusBadRequest, "This request does not support content."},
	XAmzContentSHA256Mismatch:         {XAmzContentSHA256Mismatch, http.StatusBadRequest, "The provided 'x-amz-content-sha256' header does not match what was computed."},
	AuthorizationHeaderMalformed:      {AuthorizationHeaderMalformed, http.StatusBadRequest, "The authorization header is malformed."},
	AuthorizationQueryParametersError: {AuthorizationQueryParametersError, http.StatusBadRequest, "Error parsing the X-Amz-Credential parameter."},
	ExpiredToken:                      {ExpiredToken, http.StatusBadRequest, "The provided token has expired."},
	InsufficientStorage:               {InsufficientStorage, http.StatusInsufficientStorage, "No space left on device."},
}

// Lookup returns the table row for code. Unknown codes map to InternalError so a typo can never
// produce a 200 with an error body; the returned Error keeps the requested code for the log.
func Lookup(code Code) Error {
	if e, ok := table[code]; ok {
		return e
	}
	e := table[InternalError]
	e.Code = code
	return e
}

// Codes lists every code in the table, for tests and docs.
func Codes() []Code {
	out := make([]Code, 0, len(table))
	for c := range table {
		out = append(out, c)
	}
	return out
}

// ErrorResponse is the S3 error XML body.
type ErrorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      Code     `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId,omitempty"`
	HostID    string   `xml:"HostId,omitempty"`
}

// Render produces the error XML body for code. message overrides the table message when non-empty.
func Render(code Code, message, resource, requestID, hostID string) []byte {
	e := Lookup(code)
	if message == "" {
		message = e.Message
	}
	var buf bytes.Buffer
	buf.WriteString(xml.Header)
	_ = xml.NewEncoder(&buf).Encode(ErrorResponse{ //nolint:errcheck // encoding a fixed struct to a buffer cannot fail
		Code: code, Message: message, Resource: resource, RequestID: requestID, HostID: hostID,
	})
	buf.WriteByte('\n')
	return buf.Bytes()
}

// Write sends the error response for code with the table's HTTP status.
func Write(w http.ResponseWriter, code Code, message, resource, requestID string) {
	body := Render(code, message, resource, requestID, "")
	h := w.Header()
	h.Set("Content-Type", "application/xml")
	h.Set("Content-Length", itoa(len(body)))
	if requestID != "" {
		h.Set("x-amz-request-id", requestID)
	}
	w.WriteHeader(Lookup(code).Status)
	_, _ = w.Write(body)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
