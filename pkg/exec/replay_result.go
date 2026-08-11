package exec

import (
	"fmt"
	"time"

	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
)

func validateReplayResult(prepared PreparedQuery, result *DQLQueryResponse) ([]execreplay.ValidatedResultContract, error) {
	contracts := prepared.Compilation.ResultContracts
	if len(contracts) == 0 {
		return nil, nil
	}
	if result == nil {
		return nil, fmt.Errorf("query returned no response for metric result validation")
	}
	metricMetadata := result.GetMetrics()
	metadataIndex := 0
	validated := make([]execreplay.ValidatedResultContract, 0, len(contracts))
	for _, contract := range contracts {
		source, ok := replaySourceByOrdinal(prepared.Compilation.Sources, contract.Source.Ordinal)
		if !ok || source.Source.Metric == nil {
			return nil, fmt.Errorf("metric source %d has no compiled source metadata", contract.Source.Ordinal)
		}
		fieldCount := len(source.Source.Metric.Aggregations)
		if fieldCount == 0 {
			fieldCount = 1
		}
		if metadataIndex+fieldCount > len(metricMetadata) {
			return nil, fmt.Errorf("metric source %d has incomplete result field metadata", contract.Source.Ordinal)
		}
		fields := make([]string, 0, fieldCount)
		seen := make(map[string]struct{}, fieldCount)
		for _, metric := range metricMetadata[metadataIndex : metadataIndex+fieldCount] {
			if metric.FieldName == "" {
				return nil, fmt.Errorf("metric source %d has an unnamed result field", contract.Source.Ordinal)
			}
			if _, duplicate := seen[metric.FieldName]; duplicate {
				return nil, fmt.Errorf("metric source %d repeats result field %q", contract.Source.Ordinal, metric.FieldName)
			}
			seen[metric.FieldName] = struct{}{}
			fields = append(fields, metric.FieldName)
		}
		metadataIndex += fieldCount

		observed, err := observeMetricResult(contract, ExtractQueryRecords(result), fields)
		if err != nil {
			return nil, err
		}
		value, err := execreplay.ValidateResultContract(contract, observed)
		if err != nil {
			return nil, err
		}
		validated = append(validated, value)
	}
	if metadataIndex != len(metricMetadata) {
		return nil, fmt.Errorf("query returned %d unbound metric result fields", len(metricMetadata)-metadataIndex)
	}
	return validated, nil
}

func replaySourceByOrdinal(values []execreplay.SourceCompilation, ordinal int) (execreplay.SourceCompilation, bool) {
	for _, value := range values {
		if value.Source.Ordinal == ordinal {
			return value, true
		}
	}
	return execreplay.SourceCompilation{}, false
}

func observeMetricResult(contract execreplay.ReplayResultContract, records []map[string]interface{}, fields []string) (execreplay.ObservedResultMetadata, error) {
	var (
		commonStart    time.Time
		commonEnd      time.Time
		commonInterval time.Duration
		commonPoints   = -1
		relevant       int
	)
	for recordIndex, record := range records {
		containsField := false
		for _, field := range fields {
			if _, ok := record[field]; ok {
				containsField = true
				break
			}
		}
		if !containsField {
			continue
		}
		relevant++
		timeframe, ok := record["timeframe"].(map[string]interface{})
		if !ok {
			return execreplay.ObservedResultMetadata{}, fmt.Errorf("metric result record %d has no timeframe", recordIndex)
		}
		start, startOK := parseTimeFromMap(timeframe, "start")
		end, endOK := parseTimeFromMap(timeframe, "end")
		interval, intervalOK := parseRecordInterval(record["interval"])
		if !startOK || !endOK || !intervalOK || interval <= 0 || !end.After(start) {
			return execreplay.ObservedResultMetadata{}, fmt.Errorf("metric result record %d has an invalid timeframe or natural interval", recordIndex)
		}
		pointCount := -1
		for _, field := range fields {
			values, ok := record[field].([]interface{})
			if !ok {
				return execreplay.ObservedResultMetadata{}, fmt.Errorf("metric result record %d is missing array field %q", recordIndex, field)
			}
			if pointCount == -1 {
				pointCount = len(values)
			} else if pointCount != len(values) {
				return execreplay.ObservedResultMetadata{}, fmt.Errorf("metric result record %d fields disagree on point count", recordIndex)
			}
		}
		if pointCount <= 0 || end.Sub(start) != time.Duration(pointCount)*interval {
			return execreplay.ObservedResultMetadata{}, fmt.Errorf("metric result record %d timeframe, interval, and point count disagree", recordIndex)
		}
		if commonPoints == -1 {
			commonStart, commonEnd, commonInterval, commonPoints = start, end, interval, pointCount
		} else if !commonStart.Equal(start) || !commonEnd.Equal(end) || commonInterval != interval || commonPoints != pointCount {
			return execreplay.ObservedResultMetadata{}, fmt.Errorf("metric result series disagree on timeframe, natural interval, or point count")
		}
	}
	if relevant == 0 || commonPoints <= 0 || commonInterval <= 0 {
		return execreplay.ObservedResultMetadata{}, fmt.Errorf("metric result exposes no auditable natural buckets")
	}

	buckets := make([]execreplay.MetricBucket, 0, commonPoints)
	for index := 0; index < commonPoints; index++ {
		start := commonStart.Add(time.Duration(index) * commonInterval)
		buckets = append(buckets, execreplay.MetricBucket{Range: execreplay.Interval{Start: start, End: start.Add(commonInterval)}})
	}
	physical := &execreplay.Interval{Start: commonStart.UTC(), End: commonEnd.UTC()}
	var lowerSpill, upperSpill time.Duration
	if physical.Start.Before(contract.LogicalWindow.Start) {
		lowerSpill = contract.LogicalWindow.Start.Sub(physical.Start)
	}
	if physical.End.After(contract.LogicalWindow.End) {
		upperSpill = physical.End.Sub(contract.LogicalWindow.End)
	}
	return execreplay.ObservedResultMetadata{
		Source: contract.Source, NaturalInterval: commonInterval, Buckets: buckets,
		Provenance: execreplay.ResultProvenance{
			Source: contract.Source, LogicalWindow: contract.LogicalWindow,
			PhysicalRange: physical, NaturalInterval: commonInterval,
			LowerSpill: lowerSpill, UpperSpill: upperSpill,
			BoundaryPolicy: contract.BoundaryPolicy,
		},
	}, nil
}
