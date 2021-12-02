package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/bitgo/cloudflare-logpull-exporter/pkg/logpull"
	"github.com/bitgo/cloudflare-logpull-exporter/pkg/loki"
	"github.com/prometheus/client_golang/prometheus"
)

// Zone captures both the ID and Name of a Cloudflare zone.
type Zone struct {
	ID   string
	Name string
}

// A LokiPump pulls from Logpull and pushes into Loki.
type LokiPump struct {
	logpullAPI *logpull.API
	lokiAPI    *loki.API
	// metrics    LokiPumpMetrics
}

// NewLokiPump creates a new LokiPump.
func NewLokiPump(logpullAPI *logpull.API, lokiAPI *loki.API) *LokiPump {
	return &LokiPump{
		logpullAPI: logpullAPI,
		lokiAPI:    lokiAPI,
		// metrics:    NewLokiPumpMetrics(),
	}
}

// Pump will pull some logs from Cloudflare for the given zone and then push
// them into Loki. It returns the number of logs pushed, or an error if any
// occurred.
func (pump *LokiPump) Pump(zone Zone, start time.Time, end time.Time) (int, error) {
	logReader, err := pump.logpullAPI.ZoneLogs(zone.ID, nil, 0, start, end)
	if logReader != nil {
		defer logReader.Close()
	}
	if err != nil {
		return 0, fmt.Errorf("pulling logs for zone %s: %w", zone.Name, err)
	}

	values := make([]loki.Value, 0, 1000)

	scanner := bufio.NewScanner(logReader)
	for scanner.Scan() {
		var meta struct{ EdgeEndTimestamp int64 }
		err = json.Unmarshal(scanner.Bytes(), &meta)
		if err != nil {
			return 0, fmt.Errorf("decoding log metadata: %w", err)
		}

		timestamp := time.Unix(0, meta.EdgeEndTimestamp)
		values = append(values, loki.Value{Time: timestamp, Line: scanner.Text()})
	}

	// Cloudflare API docs specify that we should not expect the received
	// logs to be in any particular order. We sort them to make Loki happy.
	sort.SliceStable(values, func(i, j int) bool {
		return values[i].Time.Before(values[j].Time)
	})

	streams := []loki.Stream{
		{
			Labels: map[string]string{
				"job":  "cloudflare-logpull-exporter",
				"zone": zone.Name,
			},
			Values: values,
		},
	}

	err = pump.lokiAPI.Push(streams)
	if err != nil {
		return 0, fmt.Errorf("pushing loki stream for zone %s: %w", zone.Name, err)
	}

	return len(values), nil
}

// LokiPumpMetrics are all of the Prometheus metrics which are captured from a
// specific LokiPump.
type LokiPumpMetrics struct {
	errors prometheus.Counter
}

// NewLokiPumpMetrics creates a new set of Prometheus metrics for a specific
// LokiPump.
func NewLokiPumpMetrics() *LokiPumpMetrics {
	return &LokiPumpMetrics{
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "loki_push_errors_total",
			Help: "The number of errors that have occurred while pushing logs to Loki",
		}),
	}
}
