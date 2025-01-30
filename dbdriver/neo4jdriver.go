package dbdriver

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const ReadTxTimeoutSeconds = 30

type Neo4jDriverParams struct {
	Host     string
	User     string
	Password string
	Port     string
}

type Neo4jDriver struct {
	driver    neo4j.DriverWithContext
	connError error
	ctx       context.Context
	// Session   neo4j.SessionWithContext
	mode string
}

type WriteSummary struct {
	NodesCreated         int
	NodesDeleted         int
	RelationshipsCreated int
	RelationshipsDeleted int
}

func logRequest(action, cypher string, params map[string]any) {
	paramsStr := ""
	for k, v := range params {
		paramsStr += fmt.Sprintf("%s=%v ", k, v)
	}
	slog.Info(fmt.Sprintf("DB Request '%s', \ncypher=\n  %s\nparams=%s", action, cypher, paramsStr))
}

func logCounters(counters neo4j.Counters) {
	nodeCountUpdated := counters.NodesCreated() + counters.NodesDeleted()
	relationshipCountUpdated := counters.RelationshipsCreated() + counters.RelationshipsDeleted()
	nodeRelationshipCountUpdated := nodeCountUpdated + relationshipCountUpdated
	propertiesChanged := counters.PropertiesSet()
	slog.Info("DB Counters",
		"changedNodesRelationships", nodeRelationshipCountUpdated,
		"changedProperties", propertiesChanged,
		"nodesCreated", counters.NodesCreated(),
		"nodesDeleted", counters.NodesDeleted(),
		"relationshipsCreated", counters.RelationshipsCreated(),
		"relationshipsDeleted", counters.RelationshipsDeleted(),
		"propertiesSet", counters.PropertiesSet(),
		"labelsAdded", counters.LabelsAdded(),
		"labelsRemoved", counters.LabelsRemoved(),
		"indexesAdded", counters.IndexesAdded(),
		"indexesRemoved", counters.IndexesRemoved(),
		"constraintsAdded", counters.ConstraintsAdded(),
		"constraintsRemoved", counters.ConstraintsRemoved(),
	)
}

func logNotifications(notifications []neo4j.Notification) {
	for ind, n := range notifications {
		slog.Info("DB Notification",
			"index", ind,
			"code", n.Code(),
			"title", n.Title(),
			"description", n.Description(),
			"position", n.Position(),
			"severity", n.SeverityLevel(),
			"code", n.Code(),
		)
	}
}

func (d *Neo4jDriver) CreateSession() neo4j.SessionWithContext {
	return d.driver.NewSession(d.ctx, neo4j.SessionConfig{})
}

func (d *Neo4jDriver) ReadTx(params DbQueryParams) (any, DriverStats, error) {
	//if config.LogCypherReadQuery {
	logRequest("ReadTx", params.Cypher, params.Params)
	//}

	driverStats := DriverStats{}
	var timeDurationSeconds time.Duration
	if params.TimeoutSeconds == nil {
		timeDurationSeconds = time.Duration(ReadTxTimeoutSeconds) * time.Second
	} else {
		timeDurationSeconds = time.Duration(*params.TimeoutSeconds) * time.Second
	}
	timeoutCtx, cancel := context.WithTimeout(d.ctx, timeDurationSeconds)
	defer cancel()

	session := d.CreateSession()
	defer session.Close(d.ctx)

	data, err := session.ExecuteRead(timeoutCtx, func(tx neo4j.ManagedTransaction) (any, error) {
		startTime := time.Now()
		res, err := tx.Run(d.ctx, params.Cypher, params.Params)
		if err != nil {
			slog.Error("DB: neo4j transaction error", "err", err, "retriable", neo4j.IsRetryable(err))
			return nil, err
		}

		records, err := res.Collect(d.ctx)
		if err != nil {
			return nil, err
		}
		for _, r := range records {
			slog.Debug(fmt.Sprintf("Data ret: %#v", r))
		}
		stats, err := res.Consume(d.ctx)
		if err != nil {
			slog.Error("getting stats", "err", err)
		}
		statsAvailable := stats.ResultAvailableAfter()
		statsConsumed := stats.ResultConsumedAfter()
		statsMeasured := time.Since(startTime)

		driverStats.MeasuredTime = statsMeasured
		driverStats.AvailableTime = statsAvailable
		driverStats.ConsumedTime = statsConsumed
		if statsAvailable > 0 {
			driverStats.ResponseTime = statsAvailable
			driverStats.ResponseTimeSeconds = statsAvailable.Seconds()
		} else {
			driverStats.ResponseTime = statsMeasured
			driverStats.ResponseTimeSeconds = statsMeasured.Seconds()
		}

		return records, nil
	})
	if err != nil {
		return nil, driverStats, err
	}

	return data, driverStats, nil
}

func (d *Neo4jDriver) WriteTx(params DbQueryParams) (bool, error) {
	retry, _, err := d.WriteCommitTx(params)
	return retry, err
}

func (d *Neo4jDriver) ReadSingleTx(params DbQueryParams) (any, error) {

	logRequest("ReadSingleTx", params.Cypher, params.Params)

	session := d.CreateSession()
	defer session.Close(d.ctx)

	res, err := session.Run(d.ctx, params.Cypher, params.Params,
		func(txConfig *neo4j.TransactionConfig) {
			txConfig.Timeout = time.Duration(ReadTxTimeoutSeconds) * time.Second
		})
	if err != nil {
		slog.Error("DB: neo4j transaction error", "err", err)
		return neo4j.IsRetryable(err), err
	}

	records, err := res.Collect(d.ctx)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (d *Neo4jDriver) WriteSingleTx(params DbQueryParams) (bool, error) {

	logRequest("WriteSingleTx", params.Cypher, params.Params)
	for k, v := range params.Params {
		slog.Info("", k, v)
	}
	session := d.CreateSession()
	defer session.Close(d.ctx)

	res, err := session.Run(d.ctx, params.Cypher, params.Params,
		func(txConfig *neo4j.TransactionConfig) {
			txConfig.Timeout = 30 * time.Second // <4>
		})
	if err != nil {
		slog.Error("DB: neo4j transaction error", "err", err)
		return neo4j.IsRetryable(err), err
	}

	cc, err := res.Consume(d.ctx)
	if err != nil {
		slog.Error("DB: neo4j transaction consume error", "err", err)
		return neo4j.IsRetryable(err), err
	}
	logCounters(cc.Counters())
	logNotifications(cc.Notifications())

	if res.Record() != nil {
		slog.Info("DB: neo4j", "record", res.Record())
	}

	return false, nil
}

func (d *Neo4jDriver) WriteCommitTx(params DbQueryParams) (bool, WriteSummary, error) {
	logRequest(" Session aware WriteCommitTx", params.Cypher, params.Params)

	session := d.CreateSession()
	defer session.Close(d.ctx)

	var nodesCreated int
	var nodesDeleted int
	var edgesCreated int
	var edgesDeleted int

	_, err := session.ExecuteWrite(d.ctx,
		func(tx neo4j.ManagedTransaction) (any, error) {
			result, err := tx.Run(d.ctx, params.Cypher, params.Params)
			if err != nil {
				return nil, err
			}
			records, err := result.Collect(d.ctx)
			if err != nil {
				return nil, err
			}
			// consume
			cc, err := result.Consume(d.ctx)
			if err != nil {
				return nil, err
			}
			nodesCreated = cc.Counters().NodesCreated()
			nodesDeleted = cc.Counters().NodesDeleted()
			edgesCreated = cc.Counters().RelationshipsCreated()
			edgesDeleted = cc.Counters().RelationshipsDeleted()

			logCounters(cc.Counters())
			logNotifications(cc.Notifications())
			return records, nil
		})
	if err != nil {
		slog.Error("DB write commit error", "error", err, "retriable", neo4j.IsRetryable(err))
	}

	return neo4j.IsRetryable(err), WriteSummary{
		NodesCreated:         nodesCreated,
		NodesDeleted:         nodesDeleted,
		RelationshipsCreated: edgesCreated,
		RelationshipsDeleted: edgesDeleted,
	}, err
}

// NewNeo4jDriver creates a new Neo4j driver
// mode is either "memgraph" or "neo4j"
func NewNeo4jDriver(mode string, params Neo4jDriverParams) DbDriver {
	driver, err := neo4j.NewDriverWithContext(
		fmt.Sprintf("bolt://%s:%s", params.Host, params.Port),
		neo4j.BasicAuth(params.User, params.Password, ""))

	ctx := context.Background()

	return &Neo4jDriver{driver: driver, connError: err, ctx: ctx, mode: mode}
}

func (d *Neo4jDriver) VerifyConnectivity() error {
	return d.driver.VerifyConnectivity(d.ctx)
}

func (d *Neo4jDriver) GetMode() string {
	return d.mode
}

func (d *Neo4jDriver) Close() {
	slog.Info("Closing driver and underlying connections!")
	d.driver.Close(d.ctx)
}
