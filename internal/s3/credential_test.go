package s3

import "testing"

func TestClassifyCredentialError(t *testing.T) {
	for _, tc := range []struct {
		code, message string
		want          CredentialFault
	}{
		{"SignatureDoesNotMatch", "The request signature we calculated does not match", FaultSignature},
		{"InvalidAccessKeyId", "The AWS access key Id you provided does not exist in our records.", FaultUnknownKey},
		{"AccessDenied", "Forbidden: Invalid signature", FaultSignature},                        // Garage 2.3.0
		{"AccessDenied", "Forbidden: No such key: GKdoesnotexist000000000000", FaultUnknownKey}, // Garage 2.3.0
		{"AccessDenied", "Access Denied", FaultNone},                                            // a real permission denial
		{"InvalidSecurity", "The provided security credentials are not valid.", FaultNone},      // VAST, for a key without the right
		{"NoSuchBucket", "", FaultNone},
		{"", "", FaultNone},
	} {
		if got := ClassifyCredentialError(tc.code, tc.message); got != tc.want {
			t.Errorf("%s %q: got %d, want %d", tc.code, tc.message, got, tc.want)
		}
	}
}

func BenchmarkClassifyCredentialError(b *testing.B) {
	for b.Loop() {
		_ = ClassifyCredentialError("AccessDenied", "Forbidden: No such key: GKdoesnotexist000000000000")
	}
}
