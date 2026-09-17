package s3

import "strings"

// CredentialFault is what a backend's error says is wrong with the credentials a request was
// signed with, as far as it can be told apart from a permission denial.
type CredentialFault int

// The faults. FaultNone covers every other answer, including AccessDenied for a key that is valid
// but not allowed the operation.
const (
	FaultNone       CredentialFault = iota
	FaultSignature                  // the access key exists, the secret is not its secret
	FaultUnknownKey                 // the backend has no such access key
)

// ClassifyCredentialError reads an S3 error's code and message. VAST, MinIO and AWS use the
// standard codes; Garage 2.3.0 answers AccessDenied for both and names the fault in the message
// ("Forbidden: Invalid signature", "Forbidden: No such key: …"; docs/reference/backend-compat.md).
func ClassifyCredentialError(code, message string) CredentialFault {
	switch code {
	case "SignatureDoesNotMatch":
		return FaultSignature
	case "InvalidAccessKeyId":
		return FaultUnknownKey
	case "AccessDenied":
		switch {
		case strings.Contains(message, "Invalid signature"):
			return FaultSignature
		case strings.Contains(message, "No such key"):
			return FaultUnknownKey
		}
	}
	return FaultNone
}
