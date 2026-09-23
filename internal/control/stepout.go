package control

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/blakegolliher/shunt/internal/auth"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/sigv4"
)

// StepOut answers GET /v1/tenants/{tenant}/step-out: whether the tenant's clients could use one
// cluster directly, with shunt out of the path, and what stands in the way (ADR-0011). It changes
// nothing.
type StepOut struct {
	Tenant    string          `json:"tenant"`
	Cluster   string          `json:"cluster,omitempty"` // the one cluster every bucket is on
	Scheme    string          `json:"scheme,omitempty"`
	Endpoints []string        `json:"endpoints,omitempty"`
	Ready     bool            `json:"ready"`
	Problems  []string        `json:"problems"` // tenant-wide
	Notes     []string        `json:"notes"`    // differences a client could notice, none blocking
	Buckets   []StepOutBucket `json:"buckets"`
	Keys      []StepOutKey    `json:"keys"`
}

// StepOutBucket is one bucket's side of the check.
type StepOutBucket struct {
	Bucket   string   `json:"bucket"`
	State    string   `json:"state"`
	Cluster  string   `json:"cluster"`
	Name     string   `json:"name"` // the bucket's name on Cluster
	Problems []string `json:"problems"`
	Notes    []string `json:"notes"`
}

// StepOutKey is one client key's side of the check: what the cluster says to that key directly.
type StepOutKey struct {
	AccessKey string   `json:"access_key"`
	Problems  []string `json:"problems"`
}

func (s *Server) stepOut(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant")
	f := s.Dir.Snapshot().File()
	out := StepOut{Tenant: tenant, Problems: []string{}, Notes: []string{}, Buckets: []StepOutBucket{}, Keys: []StepOutKey{}}
	if _, ok := f.Tenants[tenant]; !ok {
		fail(w, fmt.Errorf("%w: no tenant %s in the directory", directory.ErrNotFound, tenant))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), backendTimeout)
	defer cancel()

	var clusters []string
	for _, key := range sortedKeys(f.Placements) {
		t, bucket, _ := directory.SplitKey(key)
		if t != tenant {
			continue
		}
		p := f.Placements[key]
		b := StepOutBucket{Bucket: bucket, State: p.State, Cluster: p.Primary, Name: p.Names[p.Primary], Problems: []string{}, Notes: []string{}}
		if !slices.Contains(clusters, p.Primary) {
			clusters = append(clusters, p.Primary)
		}
		if p.State != directory.StateActive {
			b.Problems = append(b.Problems, fmt.Sprintf("bucket %s is %s: finish the move first (cutover, then purge-source or migrate finish)", key, p.State))
		}
		if p.Target != "" {
			b.Notes = append(b.Notes, fmt.Sprintf("a move of bucket %s to %s is recorded but not started; stepping out leaves it undone", key, p.Target))
		}
		if b.Name != bucket {
			b.Problems = append(b.Problems, fmt.Sprintf("bucket %s is named %s on %s: clients going direct would have to use that name, and a bucket cannot be renamed; move it once more with --name %s", key, b.Name, p.Primary, bucket))
		}
		if p.State == directory.StateActive {
			if n, err := s.uploadsInProgress(ctx, p.Primary, b.Name); err != nil {
				b.Problems = append(b.Problems, fmt.Sprintf("cannot list multipart uploads of %s on %s: %v", b.Name, p.Primary, err))
			} else if n > 0 {
				b.Problems = append(b.Problems, fmt.Sprintf("bucket %s has multipart uploads in progress on %s: their upload ids only work through shunt, so let them finish or abort them", key, p.Primary))
			}
		}
		out.Buckets = append(out.Buckets, b)
	}
	switch {
	case len(out.Buckets) == 0:
		out.Problems = append(out.Problems, "there are no buckets to step out of")
	case len(clusters) > 1:
		out.Problems = append(out.Problems, fmt.Sprintf("the buckets are on %d clusters (%s): clients going direct would need one endpoint per cluster; move them onto one first", len(clusters), strings.Join(clusters, ", ")))
	default:
		out.Cluster = clusters[0]
		c := f.Clusters[out.Cluster]
		out.Scheme, out.Endpoints = c.Scheme, endpoints(c)
		s.checkKeys(ctx, &out)
	}

	out.Ready = len(out.Problems) == 0
	for i := range out.Buckets {
		out.Ready = out.Ready && len(out.Buckets[i].Problems) == 0
	}
	for i := range out.Keys {
		out.Ready = out.Ready && len(out.Keys[i].Problems) == 0
	}
	writeJSON(w, http.StatusOK, out)
}

// checkKeys asks the cluster, as each of the tenant's client keys, what that client could do
// without shunt: the key must be known with the same secret and must reach every bucket by its
// name. Buckets the key would list that shunt does not show are notes.
func (s *Server) checkKeys(ctx context.Context, out *StepOut) {
	var keys []sigv4.Credential
	if s.Keys != nil {
		keys = s.Keys.Tenant(out.Tenant)
	}
	if len(keys) == 0 {
		out.Problems = append(out.Problems, "this shunt holds no client keys for these buckets to check")
		return
	}
	cl, err := s.backendFor(out.Cluster)
	if err != nil {
		out.Problems = append(out.Problems, err.Error())
		return
	}
	shown := map[string]bool{}
	for _, b := range out.Buckets {
		shown[b.Name] = true
	}
	for _, k := range keys {
		res := StepOutKey{AccessKey: k.AccessKey, Problems: []string{}}
		as := backend{cl: cl.cl, as: &sigv4.Credentials{AccessKey: k.AccessKey, Secret: k.Secret}}
		listed, problem := listBucketsAs(ctx, as)
		if problem != "" {
			res.Problems = append(res.Problems, problem)
			out.Keys = append(out.Keys, res)
			continue
		}
		for _, b := range out.Buckets {
			if len(k.Buckets) > 0 && !slices.Contains(k.Buckets, b.Bucket) {
				continue // shunt never let this key reach the bucket
			}
			switch ok, err := as.bucketExists(ctx, b.Name); {
			case err != nil:
				res.Problems = append(res.Problems, fmt.Sprintf("cannot use bucket %s on %s: %v", b.Name, out.Cluster, err))
			case !ok:
				res.Problems = append(res.Problems, fmt.Sprintf("bucket %s on %s does not answer this key", b.Name, out.Cluster))
			}
		}
		if len(k.Buckets) > 0 {
			out.Notes = append(out.Notes, fmt.Sprintf("access key %s is limited to %s by shunt; %s enforces its own permissions instead", k.AccessKey, strings.Join(k.Buckets, ", "), out.Cluster))
		}
		var extra []string
		for _, name := range listed {
			if !shown[name] {
				extra = append(extra, name)
			}
		}
		slices.Sort(extra) // a backend's listing order is its own; the note must not depend on it
		if len(extra) > 0 {
			out.Notes = append(out.Notes, fmt.Sprintf("listing buckets directly as %s shows %d that shunt does not: %s", k.AccessKey, len(extra), strings.Join(extra, ", ")))
		}
		out.Keys = append(out.Keys, res)
	}
}

// listBucketsAs sends ListBuckets as a client key, returning the bucket names, or why the cluster
// would not take the key.
func listBucketsAs(ctx context.Context, b backend) (names []string, problem string) {
	r, err := b.do(ctx, http.MethodGet, "", "", nil, nil, nil)
	if err != nil {
		return nil, fmt.Sprintf("cannot reach %s: %v", b.cl.Name, err)
	}
	if r.status != http.StatusOK {
		var e struct {
			Message string `xml:"Message"`
		}
		_ = xml.Unmarshal(r.body, &e) //nolint:errcheck // no message is fine
		switch s3.ClassifyCredentialError(r.code, e.Message) {
		case s3.FaultUnknownKey:
			return nil, fmt.Sprintf("%s does not know this access key: clients going direct would need keys %s issues, or shunt should hold %s's own keys from the start (docs/migrating.md)", b.cl.Name, b.cl.Name, b.cl.Name)
		case s3.FaultSignature:
			return nil, fmt.Sprintf("%s knows this access key with a different secret", b.cl.Name)
		}
		return nil, fmt.Sprintf("%s answered ListBuckets for this key with HTTP %d %s", b.cl.Name, r.status, r.code)
	}
	var page struct {
		Buckets []struct {
			Name string `xml:"Name"`
		} `xml:"Buckets>Bucket"`
	}
	if err := xml.Unmarshal(r.body, &page); err != nil {
		return nil, fmt.Sprintf("unreadable ListBuckets answer from %s: %v", b.cl.Name, err)
	}
	for _, bk := range page.Buckets {
		names = append(names, bk.Name)
	}
	return names, ""
}

// uploadsInProgress counts the in-progress multipart uploads of a bucket (one page is enough to
// know whether there are any).
func (s *Server) uploadsInProgress(ctx context.Context, cluster, bucket string) (int, error) {
	b, err := s.backendFor(cluster)
	if err != nil {
		return 0, err
	}
	r, err := b.do(ctx, http.MethodGet, bucket, "", url.Values{"uploads": {""}, "max-uploads": {"1"}}, nil, nil)
	switch {
	case err != nil:
		return 0, err
	case r.code == "NoSuchUpload":
		return 0, nil // an in-memory test backend's answer for a bucket that never had an upload
	case r.status != http.StatusOK:
		return 0, fmt.Errorf("HTTP %d %s", r.status, r.code)
	}
	var page struct {
		Uploads []struct{} `xml:"Upload"`
	}
	if err := xml.Unmarshal(r.body, &page); err != nil {
		return 0, err
	}
	return len(page.Uploads), nil
}

// ClientKeyRequest imports one key clients already use, so they keep their own credentials when
// shunt goes in front of a cluster, and when it leaves again (ADR-0012, docs/DESIGN.md §11).
type ClientKeyRequest struct {
	AccessKey string   `json:"access_key"`
	Secret    string   `json:"secret"`
	Buckets   []string `json:"buckets,omitempty"` // optional allowlist, as in the credentials file
	// Cluster checks the key there before storing it: it must be a key that cluster knows, with
	// this secret. Empty: the tenant's default cluster, or no check when it has none.
	Cluster string `json:"cluster,omitempty"`
}

// ClientKeyResult is what an import stored, never the secret.
type ClientKeyResult struct {
	AccessKey string `json:"access_key"`
	Tenant    string `json:"tenant"`
	Checked   string `json:"checked,omitempty"` // the cluster that accepted the key
	Left      int    `json:"left,omitempty"`    // after a removal: the tenant's remaining keys
}

func (s *Server) importKey(w http.ResponseWriter, r *http.Request) {
	var req ClientKeyRequest
	if !decode(w, r, &req) {
		return
	}
	res, err := s.storeKey(r.Context(), r.PathValue("tenant"), req)
	if err != nil {
		fail(w, err)
		return
	}
	s.info(actor(r), "client key imported", "tenant", res.Tenant, "access_key", res.AccessKey, "checked", res.Checked)
	writeJSON(w, http.StatusOK, res)
}

// storeKey checks a client key against a cluster and stores it. The secret is never logged, never
// returned, and reaches the proxy's credential store only after the cluster has accepted it.
func (s *Server) storeKey(ctx context.Context, tenant string, req ClientKeyRequest) (ClientKeyResult, error) {
	res := ClientKeyResult{AccessKey: req.AccessKey, Tenant: tenant}
	switch {
	case s.Keys == nil:
		return res, refuse("this shunt cannot import client keys: it has no credentials file to write to")
	case req.AccessKey == "" || req.Secret == "":
		return res, refuse("a client key needs an access key and its secret")
	}
	cluster := req.Cluster
	if cluster == "" {
		cluster = s.Dir.Snapshot().File().Tenants[tenant].DefaultCluster
	}
	if cluster != "" {
		b, err := s.backendFor(cluster)
		if err != nil {
			return res, err
		}
		cctx, cancel := context.WithTimeout(ctx, backendTimeout)
		defer cancel()
		as := backend{cl: b.cl, as: &sigv4.Credentials{AccessKey: req.AccessKey, Secret: req.Secret}}
		if _, problem := listBucketsAs(cctx, as); problem != "" {
			return res, refuse("%s", problem)
		}
		res.Checked = cluster
	}
	if err := s.Keys.Add(sigv4.Credential{AccessKey: req.AccessKey, Secret: req.Secret, Tenant: tenant, Buckets: req.Buckets}); err != nil {
		if errors.Is(err, auth.ErrDuplicateKey) {
			detail := strings.TrimPrefix(err.Error(), auth.ErrDuplicateKey.Error()+": ")
			return res, refuse("shunt already holds access key %s %s; remove it first (shunt client remove %s) to replace it", req.AccessKey, detail, req.AccessKey)
		}
		return res, err
	}
	return res, nil
}

func (s *Server) removeKey(w http.ResponseWriter, r *http.Request) {
	tenant, accessKey := r.PathValue("tenant"), r.PathValue("access_key")
	if s.Keys == nil {
		fail(w, refuse("this shunt cannot change its client keys: it has no credentials file to write to"))
		return
	}
	held := false
	for _, k := range s.Keys.Tenant(tenant) {
		held = held || k.AccessKey == accessKey
	}
	if !held {
		fail(w, notFound("no client key %s here (shunt client show lists them)", accessKey))
		return
	}
	if err := s.Keys.Remove(accessKey); err != nil {
		fail(w, err)
		return
	}
	left := len(s.Keys.Tenant(tenant))
	s.info(actor(r), "client key removed", "tenant", tenant, "access_key", accessKey, "keys_left", left)
	writeJSON(w, http.StatusOK, ClientKeyResult{AccessKey: accessKey, Tenant: tenant, Left: left})
}
