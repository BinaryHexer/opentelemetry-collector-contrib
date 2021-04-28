package sampling

import (
	"go.opentelemetry.io/collector/consumer/pdata"
)

// Decision gives the status of sampling decision.
type Decision int32

const (
	// Unspecified indicates that the status of the decision was not set yet.
	Unspecified Decision = iota
	// Pending indicates that the policy was not evaluated yet.
	Pending
	// Sampled is used to indicate that the decision was already taken
	// to sample the data.
	Sampled
	// NotSampled is used to indicate that the decision was already taken
	// to not sample the data.
	NotSampled
	// Dropped is used when data needs to be purged before the sampling policy
	// had a chance to evaluate it.
	Dropped
)

type Filter interface {
	Name() string
	ApplyForTrace(traceID pdata.TraceID, trace pdata.Traces) (Decision, error)
	ApplyForLog(traceID pdata.TraceID, log pdata.Logs) (Decision, error)
}
