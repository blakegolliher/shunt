package telemetry

import (
	"bufio"
	"os"
	"regexp"
	"strings"
	"testing"
)

// catalogNames parses the metric names from the markdown table in docs/telemetry-catalog.md.
func catalogNames(t *testing.T) map[string]bool {
	t.Helper()
	f, err := os.Open("../../docs/telemetry-catalog.md")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck // read-only
	re := regexp.MustCompile("^\\| `(shunt_[a-z0-9_]+)` \\|")
	names := map[string]bool{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if m := re.FindStringSubmatch(sc.Text()); m != nil {
			names[m[1]] = true
		}
	}
	if len(names) == 0 {
		t.Fatal("no metric rows found in docs/telemetry-catalog.md")
	}
	return names
}

// TestEveryMetricIsInTheCatalog is the CI rule from docs/DESIGN.md §2.7: a registered shunt_*
// metric without a catalog row fails. Catalog rows without a metric are reported, not fatal,
// because rows are written before the code that implements them.
func TestEveryMetricIsInTheCatalog(t *testing.T) {
	cat := catalogNames(t)
	m := NewMetrics()
	// Touch each vec so it gathers with at least one series.
	m.RequestsTotal.WithLabelValues("GetObject", "2xx", "garage", "s3").Inc()
	m.RequestDuration.WithLabelValues("GetObject").Observe(0.01)
	m.UpstreamTTFB.WithLabelValues("GetObject", "garage").Observe(0.01)
	m.BytesIn.WithLabelValues("GetObject").Add(1)
	m.BytesOut.WithLabelValues("GetObject").Add(1)
	m.Inflight.WithLabelValues("GetObject").Set(1)
	m.AuthFailures.WithLabelValues("signature").Inc()
	m.AuthDuration.WithLabelValues("header").Observe(0.0001)
	m.Compensation.WithLabelValues("sha256", "logged").Inc()
	m.RouteState.WithLabelValues("acme/data", "MIGRATING").Set(1)
	m.RampRatio.WithLabelValues("acme/data").Set(0.25)
	m.RampWrites.WithLabelValues("acme/data", "primary").Inc()
	m.FallbackReads.WithLabelValues("acme/data").Inc()
	m.DualDelete.WithLabelValues("acme/data", "both").Inc()
	m.ListingMerge.WithLabelValues("acme/data").Observe(0.01)
	m.RefusedWrites.WithLabelValues("acme/data", "hold").Inc()
	m.FleetMembers.WithLabelValues("live").Set(1)
	m.FleetStale.Set(0)
	m.FenceWait.Observe(0.01)
	m.TelemetryMerge.Observe(0.001)
	m.BundlesRetired.Set(0)
	m.InstallBackpressure.Set(0)
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	registered := map[string]bool{}
	for _, f := range families {
		name := f.GetName()
		if !strings.HasPrefix(name, "shunt_") {
			continue // go_* and process_* collectors are not catalog items
		}
		registered[name] = true
		if !cat[name] {
			t.Errorf("metric %s is registered but not in docs/telemetry-catalog.md", name)
		}
	}
	for name := range cat {
		if !registered[name] {
			t.Logf("catalog row %s has no registered metric yet", name)
		}
	}
	// POC-4's fifteen, POC-6's four for the fleet, UI-1's merge latency, and H1c's two for the
	// runtime bundle.
	if len(registered) != 22 {
		t.Errorf("H1c registers exactly twenty-two shunt_ metrics, got %d: %v", len(registered), registered)
	}
}
