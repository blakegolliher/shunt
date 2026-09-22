package web

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

type routeRecord struct {
	Method            string   `json:"method"`
	Pattern           string   `json:"pattern"`
	Verbs             []string `json:"verbs"`
	Mutation          bool     `json:"mutation"`
	ReadOnlyException string   `json:"readOnlyException"`
}

func readRoutes(t *testing.T, name string) []routeRecord {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var routes []routeRecord
	if err := json.Unmarshal(b, &routes); err != nil {
		t.Fatal(err)
	}
	return routes
}

func TestUIRouteParityWithControlAPIAndCLI(t *testing.T) {
	server := map[string]routeRecord{}
	for _, route := range readRoutes(t, "../docs/reference/control-routes.json") {
		server[route.Method+" "+route.Pattern] = route
	}
	ui := readRoutes(t, "src/api/routes.json")
	declared := map[string]bool{}
	for _, route := range ui {
		key := route.Method + " " + route.Pattern
		if declared[key] {
			t.Errorf("duplicate UI route %s", key)
		}
		declared[key] = true
		api, ok := server[key]
		if !ok {
			t.Errorf("UI reaches %s, which is absent from the generated control route table", key)
			continue
		}
		if len(api.Verbs) == 0 {
			if api.Mutation || route.Method != "GET" || strings.TrimSpace(route.ReadOnlyException) == "" {
				t.Errorf("UI route %s has no CLI verb and no named read-only decision", key)
			}
		} else if route.ReadOnlyException != "" {
			t.Errorf("UI route %s has CLI verbs %v and must not claim an exception", key, api.Verbs)
		}
	}

	// A literal fetch added to the client cannot bypass the reviewed inventory above.
	literal := regexp.MustCompile(`['\"](/v1/[A-Za-z0-9_/{}/.-]+)['\"]`)
	err := filepath.WalkDir("src", func(name string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || (!strings.HasSuffix(name, ".ts") && !strings.HasSuffix(name, ".tsx")) {
			return err
		}
		b, readErr := os.ReadFile(name)
		if readErr != nil {
			return readErr
		}
		for _, match := range literal.FindAllStringSubmatch(string(b), -1) {
			if !declared["GET "+match[1]] {
				t.Errorf("%s reaches undeclared API route GET %s", name, match[1])
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
