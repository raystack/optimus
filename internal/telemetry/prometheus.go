package telemetry

import (
	"sort"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	counterMetricMap = map[string]prometheus.Counter{}
	gaugeMetricMap   = map[string]prometheus.Gauge{}
)

func getKey(metric string, labels map[string]string) string {
	eventMetricKey := metric
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		eventMetricKey += "/" + key + ":" + labels[key]
	}
	return eventMetricKey
}

func NewCounter(metric string, labels map[string]string) prometheus.Counter {
	metricKey := getKey(metric, labels)
	if _, ok := counterMetricMap[metricKey]; !ok {
		counterMetricMap[metricKey] = promauto.NewCounter(prometheus.CounterOpts{Name: metric, ConstLabels: labels})
	}
	return counterMetricMap[metricKey]
}

func NewGauge(metric string, labels map[string]string) prometheus.Gauge {
	metricKey := getKey(metric, labels)
	if _, ok := gaugeMetricMap[metricKey]; !ok {
		gaugeMetricMap[metricKey] = promauto.NewGauge(prometheus.GaugeOpts{Name: metric, ConstLabels: labels})
	}
	return gaugeMetricMap[metricKey]
}
