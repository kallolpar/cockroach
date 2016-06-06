// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	gosql "database/sql"
	"errors"
	"fmt"
	"net/http"
	"time"

	// Postgres driver.
	_ "github.com/cockroachdb/pq"
	"github.com/montanaflynn/stats"

	"github.com/cockroachdb/cockroach/server/serverpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

const (
	stableInterval = 3 * time.Minute
)

func cockroachURL(host string, port int) string {
	return fmt.Sprintf("postgresql://root@%s:%d/system?sslmode=disable", host, port)
}

// printStats prints the time it took for rebalancing to finish and the final
// standard deviation of replica counts across stores.
func printRebalanceStats(db *gosql.DB, host string, adminPort int) error {
	// Output time it took to rebalance.
	{
		var rebalanceIntervalStr string
		q := `SELECT (SELECT MAX(timestamp) FROM rangelog) - ` +
			`(select MAX(timestamp) FROM eventlog WHERE eventType='` + string(sql.EventLogNodeJoin) + `')`
		if err := db.QueryRow(q).Scan(&rebalanceIntervalStr); err != nil {
			return err
		}
		rebalanceInterval, err := time.ParseDuration(rebalanceIntervalStr)
		if err != nil {
			return err
		}
		if rebalanceInterval < 0 {
			// This can happen with single-node clusters.
			rebalanceInterval = time.Duration(0)
		}
		log.Infof("cluster took %s to rebalance", rebalanceInterval)
	}

	// Output # of range events that occurred. All other things being equal,
	// larger numbers are worse and potentially indicate thrashing.
	{
		var rangeEvents int64
		q := `SELECT COUNT(*) from rangelog`
		if err := db.QueryRow(q).Scan(&rangeEvents); err != nil {
			return err
		}
		log.Infof("%d range events", rangeEvents)
	}

	// Output standard deviation of the replica counts for all stores.
	{
		var client http.Client
		var nodesResp serverpb.NodesResponse
		url := fmt.Sprintf("http://%s:%d/_status/nodes", host, adminPort)
		if err := util.GetJSON(client, url, &nodesResp); err != nil {
			return err
		}
		var replicaCounts stats.Float64Data
		for _, node := range nodesResp.Nodes {
			for _, ss := range node.StoreStatuses {
				replicaCounts = append(replicaCounts, float64(ss.Metrics["replicas"]))
			}
		}
		stddev, err := stats.StdDevP(replicaCounts)
		if err != nil {
			return err
		}
		log.Infof("stddev(replica count) = %.2f", stddev)
	}

	return nil
}

// checkStable returns whether the replica distribution within the cluster has
// been stable for at least `stableInterval`.
func checkRebalancerStable(db *gosql.DB) (bool, error) {
	q := `SELECT NOW() - MAX(timestamp) FROM rangelog`
	var elapsedStr string
	if err := db.QueryRow(q).Scan(&elapsedStr); err != nil {
		// Log but don't return errors, to increase resilience against transient
		// errors.
		log.Errorf("error checking rebalancer: %s", err)
		return false, nil
	}
	elapsedSinceLastRangeEvent, err := time.ParseDuration(elapsedStr)
	if err != nil {
		return false, err
	}

	var status string
	stable := elapsedSinceLastRangeEvent >= stableInterval
	if stable {
		status = "rebalancer is idle"
	} else {
		status = "waiting for rebalancer to be idle"
	}
	log.Infof("last range event occurred %s ago: %s", elapsedSinceLastRangeEvent, status)
	return stable, nil
}

// waitForRebalance waits until there's been no recent range adds, removes, and
// splits. We wait until the cluster is balanced or until `maxWait` elapses,
// whichever comes first. Then, it prints stats about the rebalancing process.
func waitForRebalance(ctx ccContext) error {
	host, err := randomTerraformOutputVal(ctx, tfCockroachIPs)
	if err != nil {
		return err
	}
	dbURL := cockroachURL(host, 26257)
	db, err := gosql.Open("postgres", dbURL)
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()
	if log.V(1) {
		log.Info("database URL = ", dbURL)
		log.Infof("timeout = %s", ctx.wrFlags.timeout)
	}

	timeoutTimer := time.After(ctx.wrFlags.timeout)
	var timer timeutil.Timer
	defer timer.Stop()
	timer.Reset(0)
	for {
		select {
		case <-timer.C:
			timer.Read = true
			stable, err := checkRebalancerStable(db)
			if err != nil {
				return err
			}
			if stable {
				if err := printRebalanceStats(db, host, 8080); err != nil {
					return err
				}
				return nil
			}
		case <-timeoutTimer:
			return errors.New("timeout expired")
		}

		timer.Reset(10 * time.Second)
	}
}
