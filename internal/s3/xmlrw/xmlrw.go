// Package xmlrw rewrites the few S3 XML elements that echo a backend's bucket name, endpoint, or
// uploadId, while streaming (docs/DESIGN.md §2.4, §9 item 11; ADR-0006). It is a byte-level scanner,
// not a parser: it tracks element nesting just far enough to know when it is inside one of the
// elements its tables name, holds that element's character data (at most MaxText bytes), asks an
// Editor for the replacement, and forwards every other byte untouched as it arrives.
//
// Identity is the primary invariant: when the Editor returns text unchanged, the bytes written to
// the destination equal the bytes written to the Writer, however the input is split across Write
// calls. Malformed or unexpected input degrades to identity, never to an error.
package xmlrw

import (
	"io"
)

// Field names what an element holds, so the Editor knows how to rewrite it.
type Field uint8

// Fields the tables below route to the Editor.
const (
	FieldBucket   Field = iota + 1 // a bucket name: replaced when it equals the backend name
	FieldUploadID                  // an uploadId: gets the cluster prefix
	FieldLocation                  // CompleteMultipartUploadResult/Location: a URL naming host and bucket
	FieldResource                  // Error/Resource: the request path, bucket segment first
	FieldMessage                   // Error/Message: free text that may name the bucket or the endpoint
	FieldEndpoint                  // Error/Endpoint: a redirect target host
)

// Editor returns the replacement for one element's character data, appended to dst. text is the
// raw (still XML-escaped) content; the replacement must be XML-escaped too. Returning text as is
// keeps the output identical to the input.
type Editor interface {
	Edit(dst []byte, f Field, text []byte) []byte
}

type rule struct {
	path  []string // element local names below the root
	field Field
	once  bool // appears at most once per document
}

type table struct {
	root  string
	rules []rule
	// repeats is true when a rule may match more than once, so the scanner cannot stop early.
	repeats bool
}

// tables is the rewrite inventory for response bodies (ADR-0006). ListAllMyBucketsResult is absent
// on purpose: shunt synthesizes ListBuckets and never relays a backend's.
var tables = []*table{
	{root: "ListBucketResult", rules: []rule{{[]string{"Name"}, FieldBucket, true}}},
	{root: "ListVersionsResult", rules: []rule{{[]string{"Name"}, FieldBucket, true}}},
	{root: "ListMultipartUploadsResult", repeats: true, rules: []rule{
		{[]string{"Bucket"}, FieldBucket, true},
		{[]string{"UploadIdMarker"}, FieldUploadID, true},
		{[]string{"NextUploadIdMarker"}, FieldUploadID, true},
		{[]string{"Upload", "UploadId"}, FieldUploadID, false},
	}},
	{root: "InitiateMultipartUploadResult", rules: []rule{{[]string{"Bucket"}, FieldBucket, true}, {[]string{"UploadId"}, FieldUploadID, true}}},
	{root: "ListPartsResult", rules: []rule{{[]string{"Bucket"}, FieldBucket, true}, {[]string{"UploadId"}, FieldUploadID, true}}},
	{root: "CompleteMultipartUploadResult", rules: []rule{{[]string{"Location"}, FieldLocation, true}, {[]string{"Bucket"}, FieldBucket, true}}},
	{root: "Error", rules: []rule{
		{[]string{"Resource"}, FieldResource, true},
		{[]string{"BucketName"}, FieldBucket, true},
		{[]string{"Bucket"}, FieldBucket, true},
		{[]string{"Message"}, FieldMessage, true},
		{[]string{"UploadId"}, FieldUploadID, true},
		{[]string{"Endpoint"}, FieldEndpoint, true},
	}},
}

// Roots lists the document roots the Writer rewrites, for tests and docs.
func Roots() []string {
	out := make([]string, len(tables))
	for i, t := range tables {
		out[i] = t.root
	}
	return out
}

const (
	maxName  = 64 // longer element names cannot match a rule
	maxDepth = 3  // deepest rule path, counting the root as depth 1
	// MaxText is the default cap on one element's held character data.
	MaxText = 64 << 10
	// tagSlack bounds the held bytes of a rewritten element's end tag ("</Name   >").
	tagSlack = 4096
)

type state uint8

const (
	stProlog     state = iota // before the first '<': whitespace and a BOM are allowed, anything else means not XML
	stText                    // character data
	stLT                      // just after '<'
	stStartName               // start tag name
	stStartAttrs              // start tag, after the name
	stStartQuote              // inside a quoted attribute value
	stEndName                 // end tag name
	stEndRest                 // end tag, after the name
	stBang                    // after "<!": deciding comment, CDATA, or declaration
	stComment                 // inside <!-- -->
	stCDATA                   // inside <![CDATA[ ]]>
	stDecl                    // inside <!DOCTYPE …> or another declaration
	stPI                      // inside <? ?>
)

// Writer is the rewriting io.Writer. It is not safe for concurrent use.
type Writer struct {
	dst     io.Writer
	ed      Editor
	maxText int

	st      state
	done    bool // pass everything from here on through
	tbl     *table
	matched []bool // once-rules already matched, by index in tbl.rules
	pending int    // once-rules not yet matched when tbl has no repeating rules

	depth   int
	names   [maxDepth + 1][maxName]byte
	nameLen [maxDepth + 1]int

	tag      [maxName]byte
	tagLen   int
	tagLong  bool
	quote    byte
	slash    bool // last significant byte of a start tag was '/'
	bang     [7]byte
	bangLen  int
	run      int // trailing '-' in a comment, ']' in CDATA, '?' in a PI
	stopNext bool

	capturing bool
	capField  Field
	capDepth  int
	capMixed  bool
	capBuf    []byte
	capLT     int // offset in the held bytes where the latest '<' began

	out []byte

	// Rewrites counts elements handed to the Editor; Overflows counts elements forwarded verbatim
	// because their text exceeded the cap or held markup.
	Rewrites, Overflows int
}

// New returns a Writer that forwards to dst and asks ed for replacements. maxText <= 0 means MaxText.
func New(dst io.Writer, ed Editor, maxText int) *Writer {
	if maxText <= 0 {
		maxText = MaxText
	}
	return &Writer{dst: dst, ed: ed, maxText: maxText}
}

// Reset reuses w for a new document.
func (w *Writer) Reset(dst io.Writer, ed Editor) {
	capBuf, out, matched := w.capBuf[:0], w.out[:0], w.matched[:0]
	*w = Writer{dst: dst, ed: ed, maxText: w.maxText, capBuf: capBuf, out: out, matched: matched}
}

// Write scans p, forwarding bytes as soon as they are known not to belong to a rewritten element.
func (w *Writer) Write(p []byte) (int, error) {
	emit := 0 // start of the bytes of p not yet forwarded or held
	for i := 0; i < len(p); i++ {
		if w.done {
			break
		}
		c := p[i]
		switch w.st {
		case stProlog:
			switch c {
			case '<':
				w.st = stLT
				w.markLT(emit, i)
			case ' ', '\t', '\r', '\n', 0xEF, 0xBB, 0xBF: // whitespace, or a UTF-8 BOM
			default:
				w.done = true // not XML: forward as is
			}
		case stText:
			if c == '<' {
				w.st = stLT
				w.markLT(emit, i)
			}
		case stLT:
			switch c {
			case '/':
				w.st, w.tagLen, w.tagLong = stEndName, 0, false
			case '?':
				w.st, w.run = stPI, 0
				w.markup()
			case '!':
				w.st, w.bangLen = stBang, 0
				w.markup()
			default:
				w.st, w.tagLen, w.tagLong, w.slash = stStartName, 0, false, false
				w.nameByte(c)
			}
		case stStartName:
			switch c {
			case '>':
				if err := w.startTag(p, &emit, i); err != nil {
					return i, err
				}
				w.st = stText
			case '/':
				w.slash, w.st = true, stStartAttrs
			default:
				if isSpace(c) {
					w.st = stStartAttrs
				} else {
					w.nameByte(c)
				}
			}
		case stStartAttrs:
			switch {
			case c == '"' || c == '\'':
				w.quote, w.slash = c, false
				w.st = stStartQuote
			case c == '>':
				if err := w.startTag(p, &emit, i); err != nil {
					return i, err
				}
				w.st = stText
			case c == '/':
				w.slash = true
			case !isSpace(c):
				w.slash = false
			}
		case stStartQuote:
			if c == w.quote {
				w.st = stStartAttrs
			}
		case stEndName:
			switch c {
			case '>':
				if err := w.endTag(p, &emit, i); err != nil {
					return i, err
				}
				w.st = stText
			default:
				if isSpace(c) {
					w.st = stEndRest
				} else {
					w.nameByte(c)
				}
			}
		case stEndRest:
			if c == '>' {
				if err := w.endTag(p, &emit, i); err != nil {
					return i, err
				}
				w.st = stText
			}
		case stBang:
			w.bang[w.bangLen] = c
			w.bangLen++
			switch b := string(w.bang[:w.bangLen]); {
			case b == "--":
				w.st, w.run = stComment, 0
			case b == "[CDATA[":
				w.st, w.run = stCDATA, 0
			case c == '>':
				w.st = stText
			case !isPrefix(b, "--") && !isPrefix(b, "[CDATA["):
				w.st = stDecl
			}
		case stComment:
			switch {
			case c == '-':
				w.run++
			case c == '>' && w.run >= 2:
				w.st = stText
			default:
				w.run = 0
			}
		case stCDATA:
			switch {
			case c == ']':
				w.run++
			case c == '>' && w.run >= 2:
				w.st = stText
			default:
				w.run = 0
			}
		case stDecl:
			if c == '>' {
				w.st = stText
			}
		case stPI:
			switch {
			case c == '?':
				w.run = 1
			case c == '>' && w.run == 1:
				w.st = stText
			default:
				w.run = 0
			}
		}
	}
	// Forward or hold the rest of p.
	if w.capturing {
		// The cap applies to the element's text, not to the part of its end tag already seen, so the
		// decision is the same however the input is split (finish applies the same two limits).
		rest := p[emit:]
		text, tail := len(w.capBuf)+len(rest), 0
		if w.st != stText {
			text, tail = w.capLT, text-w.capLT
		}
		if text > w.maxText || tail > tagSlack {
			return len(p), w.overflow(rest)
		}
		w.capBuf = append(w.capBuf, rest...)
		return len(p), nil
	}
	if emit < len(p) {
		if _, err := w.dst.Write(p[emit:]); err != nil {
			return emit, err
		}
	}
	return len(p), nil
}

// Flush forwards any held bytes verbatim. Call it after the last Write: a document cut short
// inside a rewritten element must still reach the destination byte for byte.
func (w *Writer) Flush() error {
	if !w.capturing {
		return nil
	}
	w.capturing = false
	buf := w.capBuf
	w.capBuf = w.capBuf[:0]
	if len(buf) == 0 {
		return nil
	}
	_, err := w.dst.Write(buf)
	return err
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\r' || c == '\n' }

func isPrefix(s, of string) bool { return len(s) <= len(of) && of[:len(s)] == s }

func (w *Writer) nameByte(c byte) {
	if w.tagLen < maxName {
		w.tag[w.tagLen] = c
		w.tagLen++
	} else {
		w.tagLong = true
	}
}

// localName is the tag name after any namespace prefix.
func (w *Writer) localName() []byte {
	n := w.tag[:w.tagLen]
	for i := len(n) - 1; i >= 0; i-- {
		if n[i] == ':' {
			return n[i+1:]
		}
	}
	return n
}

// markLT records where a '<' began inside a held element.
func (w *Writer) markLT(emit, i int) {
	if w.capturing {
		w.capLT = len(w.capBuf) + (i - emit)
	}
}

// markup notes that a held element contains markup (a comment, CDATA, or a PI): its bytes are then
// forwarded verbatim.
func (w *Writer) markup() {
	if w.capturing {
		w.capMixed = true
	}
}

func (w *Writer) startTag(p []byte, emit *int, i int) error {
	self := w.slash
	w.depth++
	depth := w.depth
	if self {
		w.depth--
	}
	if w.capturing {
		w.capMixed = true // a child element inside a rewritten element
		return nil
	}
	if depth == 1 {
		w.tbl = nil
		for _, t := range tables {
			if !w.tagLong && string(w.localName()) == t.root {
				w.tbl = t
				break
			}
		}
		if w.tbl == nil {
			w.done = true // not a document with anything to rewrite
			return nil
		}
		w.matched = w.matched[:0]
		for range w.tbl.rules {
			w.matched = append(w.matched, false)
		}
		w.pending = len(w.tbl.rules)
		return nil
	}
	if depth > maxDepth || w.tbl == nil {
		return nil
	}
	w.nameLen[depth] = copy(w.names[depth][:], w.localName())
	if w.tagLong || self {
		w.nameLen[depth] = -1
		return nil
	}
	for ri := range w.tbl.rules {
		r := &w.tbl.rules[ri]
		if len(r.path) != depth-1 || (r.once && w.matched[ri]) || !w.pathIs(r.path) {
			continue
		}
		if r.once {
			w.matched[ri] = true
			w.pending--
		}
		// Forward everything through the start tag, then hold the element's content.
		if _, err := w.dst.Write(p[*emit : i+1]); err != nil {
			return err
		}
		*emit = i + 1
		w.capturing, w.capField, w.capDepth, w.capMixed = true, r.field, depth, false
		w.capBuf = w.capBuf[:0]
		return nil
	}
	return nil
}

func (w *Writer) pathIs(path []string) bool {
	for k, seg := range path {
		d := k + 2
		if w.nameLen[d] != len(seg) || string(w.names[d][:w.nameLen[d]]) != seg {
			return false
		}
	}
	return true
}

func (w *Writer) endTag(p []byte, emit *int, i int) error {
	if w.capturing && w.depth == w.capDepth {
		w.capBuf = append(w.capBuf, p[*emit:i+1]...)
		held := w.capBuf
		*emit = i + 1
		w.capturing = false
		w.capBuf = held[:0]
		if err := w.finish(held); err != nil {
			return err
		}
		if !w.tbl.repeats && w.pending == 0 {
			w.stopNext = true
		}
	}
	w.depth--
	if w.depth <= 0 {
		w.done = true // after the root closes only whitespace remains
	}
	if w.stopNext && !w.capturing {
		w.done = true // every rule has matched; the rest of the document passes through unscanned
	}
	return nil
}

// finish rewrites one held element: held is its content followed by its end tag.
func (w *Writer) finish(held []byte) error {
	text, end := held[:w.capLT], held[w.capLT:]
	if w.capMixed || len(text) > w.maxText || len(end) > tagSlack {
		w.Overflows++
		_, err := w.dst.Write(held)
		return err
	}
	w.Rewrites++
	w.out = w.ed.Edit(w.out[:0], w.capField, text)
	w.out = append(w.out, end...)
	_, err := w.dst.Write(w.out)
	return err
}

// overflow forwards a held element verbatim when its text passes the cap, then keeps scanning.
func (w *Writer) overflow(rest []byte) error {
	w.Overflows++
	w.capturing = false
	if len(w.capBuf) > 0 {
		if _, err := w.dst.Write(w.capBuf); err != nil {
			return err
		}
	}
	w.capBuf = w.capBuf[:0]
	_, err := w.dst.Write(rest)
	return err
}
