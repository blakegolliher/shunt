package verify

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/blakegolliher/shunt/internal/telemetry"
)

// TelemetryComparison is the verify tool's exact-window comparison with the control API.
type TelemetryComparison struct {
	WindowStart     time.Time `json:"window_start"`
	ClientP99US     int64     `json:"client_p99_us"`
	FleetP99US      int64     `json:"fleet_p99_us"`
	DifferencePct   float64   `json:"difference_percent"`
	TolerancePct    float64   `json:"tolerance_percent"`
	WithinTolerance bool      `json:"within_tolerance"`
}

type latestTelemetry struct {
	Windows []telemetry.ScopeWindow `json:"windows"`
}

func clientWindowP99(w *telemetry.Window) (int64, error) {
	if w == nil {
		return 0, fmt.Errorf("the verify run did not complete a 10-second telemetry window")
	}
	s := telemetry.NewStore(time.Second)
	if _, err := s.Ingest([]telemetry.MemberWindow{{ID: "verify", Live: true, Telemetry: w}}); err != nil {
		return 0, err
	}
	for _, sw := range s.Latest() {
		if sw.Scope != "fleet" {
			continue
		}
		for _, summary := range sw.Series {
			if summary.Series == telemetry.SeriesClientTotal && summary.Op == telemetry.OpAll {
				return summary.P99, nil
			}
		}
	}
	return 0, fmt.Errorf("the verify window has no client_total p99")
}

// CompareTelemetry waits for the control node to publish the same completed wall-clock window,
// then compares fleet client_total p99 with the verify client's own p99.
func CompareTelemetry(ctx context.Context, baseURL, token string, window *telemetry.Window, tolerance float64) (*TelemetryComparison, error) {
	if tolerance <= 0 {
		tolerance = 10
	}
	clientP99, err := clientWindowP99(window)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/v1/telemetry/latest", http.NoBody)
		if err != nil {
			return nil, err
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			data, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
			_ = resp.Body.Close()
			if readErr != nil {
				return nil, readErr
			}
			if resp.StatusCode >= 300 {
				return nil, fmt.Errorf("telemetry API: HTTP %d %s", resp.StatusCode, strings.TrimSpace(string(data)))
			}
			var latest latestTelemetry
			if err := json.Unmarshal(data, &latest); err != nil {
				return nil, fmt.Errorf("telemetry API: %w", err)
			}
			for _, sw := range latest.Windows {
				if sw.Scope != "fleet" || !sw.Start.Equal(window.Start) {
					continue
				}
				for _, summary := range sw.Series {
					if summary.Series != telemetry.SeriesClientTotal || summary.Op != telemetry.OpAll {
						continue
					}
					diff := 0.0
					if clientP99 > 0 {
						diff = 100 * math.Abs(float64(summary.P99-clientP99)) / float64(clientP99)
					}
					return &TelemetryComparison{WindowStart: window.Start, ClientP99US: clientP99, FleetP99US: summary.P99,
						DifferencePct: diff, TolerancePct: tolerance, WithinTolerance: diff <= tolerance}, nil
				}
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("telemetry API did not publish fleet window %s within 10s", window.Start.Format(time.RFC3339))
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}
