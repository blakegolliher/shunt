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
	m.RequestsTotal.WithLabelValues("GetObject", "2xx").Inc()
	m.RequestDuration.WithLabelValues("GetObject").Observe(0.01)
	m.UpstreamTTFB.WithLabelValues("GetObject", "garage").Observe(0.01)
	m.BytesIn.WithLabelValues("GetObject").Add(1)
	m.BytesOut.WithLabelValues("GetObject").Add(1)
	m.Inflight.WithLabelValues("GetObject").Set(1)
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
	if len(registered) != 6 {
		t.Errorf("POC-1 registers exactly six shunt_ metrics, got %d: %v", len(registered), registered)
	}
}
