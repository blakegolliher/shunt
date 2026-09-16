package s3

import (
	"net/http"
	"net/url"
)

// rule is one row of the classifier table. A row matches when the method and level match,
// every key in query is present, the optional value pair matches exactly, and the optional
// header is present. Rows are walked in order; the first match wins.
type rule struct {
	method string
	level  Level
	query  []string
	value  [2]string // key, exact value; empty key means unused
	header string
	op     Op
}

const hdrCopySource = "X-Amz-Copy-Source"

// subresource lists bucket-level query keys with their GET/PUT/DELETE operations. Zero means
// the verb has no operation for that key and falls through.
type subresource struct {
	key         string
	get, put, d Op
}

var bucketSubresources = []subresource{
	{"location", OpGetBucketLocation, 0, 0},
	{"tagging", OpGetBucketTagging, OpPutBucketTagging, OpDeleteBucketTagging},
	{"ownershipControls", OpGetBucketOwnershipControls, OpPutBucketOwnershipControls, OpDeleteBucketOwnershipControls},
	{"versioning", OpGetBucketVersioning, OpPutBucketVersioning, 0},
	{"policyStatus", OpGetBucketPolicyStatus, 0, 0},
	{"policy", OpGetBucketPolicy, OpPutBucketPolicy, OpDeleteBucketPolicy},
	{"cors", OpGetBucketCors, OpPutBucketCors, OpDeleteBucketCors},
	{"object-lock", OpGetObjectLockConfiguration, OpPutObjectLockConfiguration, 0},
	{"acl", OpGetBucketAcl, OpPutBucketAcl, 0},
	{"uploads", OpListMultipartUploads, 0, 0},
	{"versions", OpListObjectVersions, 0, 0},
	{"encryption", OpGetBucketEncryption, OpPutBucketEncryption, OpDeleteBucketEncryption},
	{"lifecycle", OpGetBucketLifecycleConfiguration, OpPutBucketLifecycleConfiguration, OpDeleteBucketLifecycle},
	{"logging", OpGetBucketLogging, OpPutBucketLogging, 0},
	{"requestPayment", OpGetBucketRequestPayment, OpPutBucketRequestPayment, 0},
	{"replication", OpGetBucketReplication, OpPutBucketReplication, OpDeleteBucketReplication},
	{"publicAccessBlock", OpGetPublicAccessBlock, OpPutPublicAccessBlock, OpDeletePublicAccessBlock},
	{"notification", OpGetBucketNotificationConfiguration, OpPutBucketNotificationConfiguration, 0},
	{"accelerate", OpGetBucketAccelerateConfiguration, OpPutBucketAccelerateConfiguration, 0},
	{"website", OpGetBucketWebsite, OpPutBucketWebsite, OpDeleteBucketWebsite},
}

// configured lists the bucket-level "configuration" families: GET with ?id is Get…, GET without
// is List…; PUT and DELETE carry ?id.
type configured struct {
	key               string
	get, list, put, d Op
}

var bucketConfigured = []configured{
	{"analytics", OpGetBucketAnalyticsConfiguration, OpListBucketAnalyticsConfigurations, OpPutBucketAnalyticsConfiguration, OpDeleteBucketAnalyticsConfiguration},
	{"intelligent-tiering", OpGetBucketIntelligentTieringConfiguration, OpListBucketIntelligentTieringConfigurations, OpPutBucketIntelligentTieringConfiguration, OpDeleteBucketIntelligentTieringConfiguration},
	{"inventory", OpGetBucketInventoryConfiguration, OpListBucketInventoryConfigurations, OpPutBucketInventoryConfiguration, OpDeleteBucketInventoryConfiguration},
	{"metrics", OpGetBucketMetricsConfiguration, OpListBucketMetricsConfigurations, OpPutBucketMetricsConfiguration, OpDeleteBucketMetricsConfiguration},
}

// rules is the table. Built once; order is precedence. Comments name the trap each order avoids.
var rules = buildRules()

func buildRules() []rule {
	var t []rule
	add := func(r rule) { t = append(t, r) }

	// Service.
	add(rule{method: http.MethodGet, level: LevelService, op: OpListBuckets})

	// Bucket GET: configuration families first (?key&id before ?key), then subresources,
	// then list-type=2 by exact value, then ListObjects v1 as the fallback.
	for _, c := range bucketConfigured {
		add(rule{method: http.MethodGet, level: LevelBucket, query: []string{c.key, "id"}, op: c.get})
		add(rule{method: http.MethodGet, level: LevelBucket, query: []string{c.key}, op: c.list})
		add(rule{method: http.MethodPut, level: LevelBucket, query: []string{c.key}, op: c.put})
		add(rule{method: http.MethodDelete, level: LevelBucket, query: []string{c.key}, op: c.d})
	}
	for _, s := range bucketSubresources {
		if s.get != 0 {
			add(rule{method: http.MethodGet, level: LevelBucket, query: []string{s.key}, op: s.get})
		}
		if s.put != 0 {
			add(rule{method: http.MethodPut, level: LevelBucket, query: []string{s.key}, op: s.put})
		}
		if s.d != 0 {
			add(rule{method: http.MethodDelete, level: LevelBucket, query: []string{s.key}, op: s.d})
		}
	}
	add(rule{method: http.MethodGet, level: LevelBucket, value: [2]string{"list-type", "2"}, op: OpListObjectsV2})
	add(rule{method: http.MethodGet, level: LevelBucket, op: OpListObjects})
	add(rule{method: http.MethodHead, level: LevelBucket, op: OpHeadBucket})
	add(rule{method: http.MethodPut, level: LevelBucket, op: OpCreateBucket})
	add(rule{method: http.MethodDelete, level: LevelBucket, op: OpDeleteBucket})
	add(rule{method: http.MethodPost, level: LevelBucket, query: []string{"delete"}, op: OpDeleteObjects})
	add(rule{method: http.MethodPost, level: LevelBucket, op: OpPostObject})

	// Object GET.
	add(rule{method: http.MethodGet, level: LevelObject, query: []string{"acl"}, op: OpGetObjectAcl})
	add(rule{method: http.MethodGet, level: LevelObject, query: []string{"attributes"}, op: OpGetObjectAttributes})
	add(rule{method: http.MethodGet, level: LevelObject, query: []string{"legal-hold"}, op: OpGetObjectLegalHold})
	add(rule{method: http.MethodGet, level: LevelObject, query: []string{"retention"}, op: OpGetObjectRetention})
	add(rule{method: http.MethodGet, level: LevelObject, query: []string{"tagging"}, op: OpGetObjectTagging})
	add(rule{method: http.MethodGet, level: LevelObject, query: []string{"uploadId"}, op: OpListParts})
	add(rule{method: http.MethodGet, level: LevelObject, query: []string{"torrent"}, op: OpGetObjectTorrent})
	// Object-level ?uploads / ?versions are bucket listings misaddressed; leave them Unknown.
	add(rule{method: http.MethodGet, level: LevelObject, query: []string{"uploads"}, op: OpUnknown})
	add(rule{method: http.MethodGet, level: LevelObject, query: []string{"versions"}, op: OpUnknown})
	add(rule{method: http.MethodGet, level: LevelObject, op: OpGetObject}) // partNumber, versionId, Range are not discriminators

	// Object HEAD / DELETE.
	add(rule{method: http.MethodHead, level: LevelObject, op: OpHeadObject})
	add(rule{method: http.MethodDelete, level: LevelObject, query: []string{"uploadId"}, op: OpAbortMultipartUpload})
	add(rule{method: http.MethodDelete, level: LevelObject, query: []string{"tagging"}, op: OpDeleteObjectTagging})
	add(rule{method: http.MethodDelete, level: LevelObject, op: OpDeleteObject})

	// Object POST.
	add(rule{method: http.MethodPost, level: LevelObject, query: []string{"uploads"}, op: OpCreateMultipartUpload})
	add(rule{method: http.MethodPost, level: LevelObject, query: []string{"uploadId"}, op: OpCompleteMultipartUpload})
	add(rule{method: http.MethodPost, level: LevelObject, query: []string{"restore"}, op: OpRestoreObject})
	add(rule{method: http.MethodPost, level: LevelObject, query: []string{"select"}, value: [2]string{"select-type", "2"}, op: OpSelectObjectContent})

	// Object PUT: subresources, then uploadId+partNumber (with and without copy-source) BEFORE the
	// bare copy-source test, otherwise UploadPartCopy collapses into CopyObject.
	add(rule{method: http.MethodPut, level: LevelObject, query: []string{"tagging"}, op: OpPutObjectTagging})
	add(rule{method: http.MethodPut, level: LevelObject, query: []string{"retention"}, op: OpPutObjectRetention})
	add(rule{method: http.MethodPut, level: LevelObject, query: []string{"legal-hold"}, op: OpPutObjectLegalHold})
	add(rule{method: http.MethodPut, level: LevelObject, query: []string{"acl"}, op: OpPutObjectAcl})
	add(rule{method: http.MethodPut, level: LevelObject, query: []string{"uploadId", "partNumber"}, header: hdrCopySource, op: OpUploadPartCopy})
	add(rule{method: http.MethodPut, level: LevelObject, query: []string{"uploadId", "partNumber"}, op: OpUploadPart})
	add(rule{method: http.MethodPut, level: LevelObject, header: hdrCopySource, op: OpCopyObject})
	add(rule{method: http.MethodPut, level: LevelObject, op: OpPutObject})

	return t
}

// classify maps a request to an operation. OPTIONS at any level is a CORS preflight; anything
// that matches no row is OpUnknown and is still proxied.
func classify(method string, level Level, q url.Values, h http.Header) Op {
	if method == http.MethodOptions {
		return OpPreflight
	}
	for i := range rules {
		r := &rules[i]
		if r.method != method || r.level != level {
			continue
		}
		if !matches(r, q, h) {
			continue
		}
		return r.op
	}
	return OpUnknown
}

func matches(r *rule, q url.Values, h http.Header) bool {
	for _, k := range r.query {
		if _, ok := q[k]; !ok {
			return false
		}
	}
	if r.value[0] != "" && q.Get(r.value[0]) != r.value[1] {
		return false
	}
	if r.header != "" && h.Get(r.header) == "" {
		return false
	}
	return true
}
