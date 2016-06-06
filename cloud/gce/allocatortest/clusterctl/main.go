// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of tre License at
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
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

// ccContext is a collection of passed-in command-line parameters and methods
// based on those.
type ccContext struct {
	// Populated while command-line parsing.
	tfdir        string
	namePrefix   string
	identityFile string
	vars         sliceFlag

	pushFlags struct {
		tempURL string
	}

	wrFlags struct {
		timeout time.Duration
	}
}

func basename(path string) string {
	_, filename := filepath.Split(path)
	return filename[0 : len(filename)-len(filepath.Ext(filename))]
}

// sliceFlag is a flag type that inserts all values for a multiply-defined flag
// into the underlying slice.
type sliceFlag []string

func (sf *sliceFlag) String() string {
	return fmt.Sprintf("%#v", *sf)
}

func (sf *sliceFlag) Set(v string) error {
	*sf = append(*sf, v)
	return nil
}

func (sf *sliceFlag) Type() string {
	return "sliceFlag"
}

var _ pflag.Value = &sliceFlag{}

func addPersistentFlags(rootCmd *cobra.Command, ctx *ccContext) {
	// Map any flags registered in the standard "flag" package to the
	// root command.
	pf := rootCmd.PersistentFlags()
	flag.VisitAll(func(f *flag.Flag) {
		flag := pflag.PFlagFromGoFlag(f)
		if flag.Name == "verbosity" {
			pf.AddFlag(flag)
		}
	})

	pf.StringVarP(&ctx.tfdir, "tf-dir", "d", "./terraform", "path to Terraform plan")
	pf.StringVarP(&ctx.namePrefix, "name-prefix", "p", "", "prefix to prepend to all Terraform resource names")

	// Add Google Cloud flags.
	pf.StringVarP(&ctx.identityFile, "identity-file", "i", "google_compute_engine",
		"filename for your GCE SSH private key (omit preceding ~/.ssh/)")

	pf.VarP(&ctx.vars, "var", "v",
		"specifies an override for Terraform input variables; can be specified multiple times and "+
			"must be in the format 'key=value'")
}

func main() {
	rand.Seed(timeutil.Now().UnixNano())

	// Create context to store info parsed command-line parameters.
	var ctx ccContext

	// Setup cobra commands.
	var rootCmd = &cobra.Command{
		Use:   "clusterctl",
		Short: "clusterctl administers and performs operations on allocator test clusters",
	}

	// Add common logging and Terraform flags.
	addPersistentFlags(rootCmd, &ctx)

	// Terraform commands.
	createCmd := &cobra.Command{
		Use:   "create [number of nodes]",
		Short: "create a test cluster",
		RunE: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("number of nodes in the cluster must be specified")
			}
			return resizeCluster(ctx, args[0])
		},
	}
	outputCmd := &cobra.Command{
		Use:   "output",
		Short: "show Terraform output (IPs and URLs)",
		RunE: func(_ *cobra.Command, _ []string) error {
			return execTerraform(ctx, "output", nil /*stdout*/, nil /*extraArgs*/)
		},
	}
	destroyCmd := &cobra.Command{
		Use:   "destroy",
		Short: "destroy a test cluster",
		RunE: func(_ *cobra.Command, _ []string) error {
			return execTerraform(ctx, "destroy", nil /*stdout*/, nil /*extraArgs*/)
		},
	}
	rootCmd.AddCommand(createCmd, outputCmd, destroyCmd)

	// Process administration commands.
	startCmd := &cobra.Command{
		Use:   "start",
		Short: "start CockroachDB or client load",
	}
	restartCmd := &cobra.Command{
		Use:   "restart",
		Short: "restart CockroachDB or client load",
	}
	stopCmd := &cobra.Command{
		Use:   "stop",
		Short: "stop CockroachDB or client load",
	}
	// Add start/restart/stop commands for CockroachDB and load generators.
	procToOutputVars := map[string]tfOutputVar{
		"cockroach":    tfCockroachIPs,
		"block_writer": tfBlockWriterIPs,
	}
	for proc, outputVar := range procToOutputVars {
		for _, cmd := range []*cobra.Command{startCmd, restartCmd, stopCmd} {
			action := strings.Split(cmd.Use, " ")[0]
			func(proc string, outputVar tfOutputVar) {
				cmd.AddCommand(&cobra.Command{
					Use:   proc,
					Short: action + " " + proc + " instance(s)",
					RunE: func(_ *cobra.Command, _ []string) error {
						return dshSupervisorctl(ctx, outputVar, action, proc)
					},
				})
			}(proc, outputVar)
		}
	}
	rootCmd.AddCommand(startCmd, restartCmd, stopCmd)

	// Commands for pushing files to the cluster.
	pushCockroachCmd := &cobra.Command{
		Use:   "cockroach [path/to/cockroach/binary]",
		Short: "push a new cockroach binary to cluster and restart CockroachDB",
		Long: "Push a new cockroach binary to cluster and restart CockroachDB. By default, " +
			"this uses the latest official binary but accepts an optional path to a custom " +
			"cockroach binary.",
		RunE: func(_ *cobra.Command, args []string) error {
			switch len(args) {
			case 0:
				return downloadCockroach(ctx)
			case 1:
				return pushCockroach(ctx, args[0])
			default:
				return errors.New("path to cockroach binary must be specified")
			}
		},
	}
	pushFile := &cobra.Command{
		Use:   "file [path/to/file]",
		Short: "push a new file to CockroachDB nodes",
		RunE: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("path to cockroach binary must be specified")
			}
			return pushFile(ctx, args[0])
		},
	}
	pushCmd := &cobra.Command{
		Use:   "push",
		Short: "push new binaries to the cluster",
	}
	pushCmd.PersistentFlags().StringVarP(
		&ctx.pushFlags.tempURL, "temp-url", "t", "",
		"Google Cloud Storage URL (gs://...) for storing temp files")

	pushCmd.AddCommand(pushCockroachCmd, pushFile)
	rootCmd.AddCommand(pushCmd)

	// Upload/download stores.
	uploadCmd := &cobra.Command{
		Use:   "upload [dirname]",
		Short: "upload all stores (stops CockroachDB) to Google Cloud Storage",
		RunE: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("GCS destination directory must be specified")
			}
			return uploadStores(ctx, args[0])
		},
	}
	downloadCmd := &cobra.Command{
		Use:   "download [dirname]",
		Short: "download all stores (stops CockroachDB) from Google Cloud Storage",
		RunE: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("GCS source directory must be specified")
			}
			return downloadStores(ctx, args[0])
		},
	}
	rootCmd.AddCommand(uploadCmd, downloadCmd)

	// Resize cluster.
	resizeCmd := &cobra.Command{
		Use:   "resize [num_nodes]",
		Short: "resizes cluster to the specified number of nodes",
		RunE: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("new number of nodes must be specified")
			}
			return resizeCluster(ctx, args[0])
		},
	}
	rootCmd.AddCommand(resizeCmd)

	// Commands for taking action on a randomly chosen node.
	sqlCmd := &cobra.Command{
		Use:   "sql",
		Short: "opens a SQL shell to a randomly chosen CockroachDB node",
		RunE: func(_ *cobra.Command, args []string) error {
			return sqlShell(ctx)
		},
	}
	adminCmd := &cobra.Command{
		Use:   "admin",
		Short: "opens the web admin UI for a randomly chosen CockroachDB node",
		RunE: func(_ *cobra.Command, args []string) error {
			return openAdmin(ctx)
		},
	}
	rootCmd.AddCommand(sqlCmd, adminCmd)

	// Wait for replica rebalancing to finish.
	var timeoutStr string
	waitRebalanceCmd := &cobra.Command{
		Use:   "wait_rebalance",
		Short: `wait_rebalance waits until a cluster is balanced then prints stats`,
		Long: `wait_rebalance waits until a cluster is balanced with respect to replica counts then
		prints stats`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			ctx.wrFlags.timeout, err = time.ParseDuration(timeoutStr)
			if err != nil {
				return util.Errorf("couldn't parse max-wait: %v", err)
			}
			return waitForRebalance(ctx)
		},
	}
	waitRebalanceCmd.Flags().StringVarP(&timeoutStr, "timeout", "w", "5h",
		"max time to wait for rebalance")
	rootCmd.AddCommand(waitRebalanceCmd)

	// Verify cluster state.
	verifyCmd := &cobra.Command{
		Use:          "verify",
		Short:        "verifies state of cluster",
		Long:         "verify verifies the state of the cluster",
		SilenceUsage: true,
	}
	verifyUpCmd := &cobra.Command{
		Use:          "up",
		Short:        "up verifies that all nodes are responding to SQL queries!",
		SilenceUsage: true,
		RunE: func(_ *cobra.Command, _ []string) error {
			return verifyUp(ctx)
		},
	}
	verifyCmd.AddCommand(verifyUpCmd)
	rootCmd.AddCommand(verifyCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("%s", err)
	}
}
