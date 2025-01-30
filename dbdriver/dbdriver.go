package dbdriver

import "time"

type DbQueryParams struct {
	Params         map[string]any
	Cypher         string
	TimeoutSeconds *int64
}

type DriverStats struct {
	ResponseTime        time.Duration
	MeasuredTime        time.Duration
	AvailableTime       time.Duration
	ConsumedTime        time.Duration
	ResponseTimeSeconds float64
}

// Write*Tx functions return bool, error indicating if the query should be retried in case of error
type DbDriver interface {
	ReadTx(DbQueryParams) (any, DriverStats, error)
	WriteTx(DbQueryParams) (bool, error)
	WriteCommitTx(DbQueryParams) (bool, WriteSummary, error)
	WriteSingleTx(DbQueryParams) (bool, error)
	ReadSingleTx(DbQueryParams) (any, error)
	VerifyConnectivity() error
	GetMode() string
	Close()
}
