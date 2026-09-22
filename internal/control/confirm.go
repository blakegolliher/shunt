package control

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// Confirmation tokens (ADR-0017). A destructive action's dry run answers a token bound to what it
// saw: HMAC-SHA256 over the action's scope, a digest of the records the action depends on, and
// the issue time. The real call must present it, and it stops matching when those records change
// or after confirmTTL. Tokens are stateless: any control node with the key verifies any node's.

const confirmTTL = 10 * time.Minute

// confirmKey returns the HMAC key: ConfirmKey, or a per-process random one for a lab.
func (s *Server) confirmKey() []byte {
	s.confirmOnce.Do(func() {
		if len(s.ConfirmKey) == 0 {
			s.ConfirmKey = make([]byte, 32)
			_, _ = rand.Read(s.ConfirmKey) //nolint:errcheck // crypto/rand does not fail short
		}
	})
	return s.ConfirmKey
}

// digestOf hashes the JSON of each part, in order: what a token is bound to.
func digestOf(parts ...any) [32]byte {
	h := sha256.New()
	for _, p := range parts {
		raw, _ := json.Marshal(p) //nolint:errcheck // records marshal; a failure hashes "null", which still binds
		h.Write(raw)
		h.Write([]byte{0})
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func (s *Server) confirmMAC(scope string, digest [32]byte, issued int64) []byte {
	mac := hmac.New(sha256.New, s.confirmKey())
	mac.Write([]byte(scope + "|" + hex.EncodeToString(digest[:]) + "|" + strconv.FormatInt(issued, 10)))
	return mac.Sum(nil)
}

// confirmToken issues a token for scope over digest, and says when it expires.
func (s *Server) confirmToken(scope string, digest [32]byte) (token string, expires time.Time) {
	now := s.now()
	issued := now.Unix()
	return strconv.FormatInt(issued, 10) + "." + base64.RawURLEncoding.EncodeToString(s.confirmMAC(scope, digest, issued)), now.Add(confirmTTL).UTC()
}

// checkToken verifies a token for scope against the current digest.
func (s *Server) checkToken(token, scope string, digest [32]byte) error {
	if token == "" {
		return refuse("%s needs the confirmation token from its dry run; the token is valid for %s", scope, confirmTTL)
	}
	issuedText, macText, ok := strings.Cut(token, ".")
	issued, perr := strconv.ParseInt(issuedText, 10, 64)
	mac, derr := base64.RawURLEncoding.DecodeString(macText)
	if !ok || perr != nil || derr != nil {
		return refuse("the confirmation token is malformed; run the dry run again")
	}
	age := s.now().Sub(time.Unix(issued, 0))
	if age > confirmTTL || age < -time.Minute {
		return refuse("the confirmation token has expired; run the dry run again")
	}
	if subtle.ConstantTimeCompare(mac, s.confirmMAC(scope, digest, issued)) != 1 {
		return refuse("the confirmation token does not match the current state: something changed since the dry run; run it again")
	}
	return nil
}
