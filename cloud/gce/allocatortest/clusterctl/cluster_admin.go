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
	"bytes"
	gosql "database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/timeutil"

	// Import postgres driver.
	_ "github.com/cockroachdb/pq"
)

func verifyDSH() error {
	_, err := exec.LookPath("dsh")
	return err
}

// gceIdentityFile returns the path of the specified SSH identity file for GCE.
func gceIdentityFile(ctx ccContext) (string, error) {
	u, err := user.Current()
	if err != nil {
		return "", err
	}
	return filepath.Join(u.HomeDir, ".ssh", ctx.identityFile), nil
}

// dshCluster uses dsh to run the given command line on the machines identified
// by the specified Terraform output field.
func dshCluster(ctx ccContext, outputVar tfOutputVar, dshCmdline string) error {
	// Get the IP addresses for all CockroachDB nodes.
	if log.V(1) {
		log.Info("getting IP addresses for ", outputVar)
	}
	if err := verifyDSH(); err != nil {
		return err
	}

	var outbuf bytes.Buffer
	tfCmd := exec.Command("terraform", terraformArgs(ctx, "output")...)
	tfCmd.Stdout = &outbuf
	tfCmd.Stderr = os.Stderr
	tfCmd.Dir = ctx.tfdir
	if log.V(1) {
		log.Infof("running: %#v", tfCmd.Args)
	}
	if err := tfCmd.Run(); err != nil {
		return util.Errorf("terraform output error: %v", err)
	}

	// Get a list
	ipAddrVal, err := terraformOutputVal(ctx, outputVar)
	if err != nil {
		return err
	}
	ipAddrs := strings.Split(ipAddrVal, ",")
	var userHosts []string
	for _, ip := range ipAddrs {
		userHosts = append(userHosts, "ubuntu@"+ip)
	}

	// Run dsh command across cluster.
	identityFile, err := gceIdentityFile(ctx)
	if err != nil {
		return err
	}
	dshArgs := []string{
		"-r", "ssh",
		"-o", "-q", // Suppress huge REMOTE HOST IDENTIFICATION HAS CHANGED banner
		"-o", "-o StrictHostKeyChecking no",
		"-o", "-i" + identityFile,
		"-Mc",
		"-m", strings.Join(userHosts, ","),
		"--",
		dshCmdline,
	}
	if log.V(1) {
		log.Infof("running dsh: %v", dshArgs)
	}
	dsh := exec.Command("dsh", dshArgs...)
	dsh.Stdout = os.Stdout
	dsh.Stderr = os.Stderr
	if err := dsh.Run(); err != nil {
		return util.Errorf("dsh error: %v", err)
	}
	return nil
}

// dshSupervisorctl uses dsh to run supervisorctl across the cluster for the
// set of machines specified by outField. Note that this suppresses supervisor's
// "not running" error, because that's not fatal.
func dshSupervisorctl(ctx ccContext, outField tfOutputVar, action, procname string) error {
	if log.V(1) {
		log.Infof("%s %s", action, procname)
	}
	if err := verifyDSH(); err != nil {
		return err
	}
	supervisorCmdline := fmt.Sprintf("supervisorctl -c supervisor.conf %s %s", action, procname)
	return dshCluster(ctx, outField, supervisorCmdline)
}

// pushBinary does the heavy lifting for copying binaries to machines in the
// cluster. It creates a uniquely named copy of the file then symlinks it to
// its "normal" name. For example, `cockroach` might be copied as
// `cockroach-20160525-163901-cdo` and symlinked to `cockroach`. This retains
// an archive of prior binary pushes.
func pushBinary(ctx ccContext, outputVar tfOutputVar, path string) error {
	if _, err := os.Stat(path); err != nil {
		return util.Errorf("error opening source binary: %v", err)
	}
	if len(ctx.pushFlags.tempURL) == 0 {
		return errors.New("--temp-url/-t must be specified")
	}

	// Create unique name for binary.
	u, err := user.Current()
	if err != nil {
		return err
	}
	nowStr := timeutil.Now().UTC().Format("20060102-150405")
	binName := basename(path)
	// It'd be great to include git short rev here, but that's hard given that the
	// binary will often be for a different OS than clusterctl is running on.
	uniqueName := fmt.Sprintf("%s.%s-%s", binName, nowStr, u.Username)
	gcsPath := ctx.pushFlags.tempURL + "/" + uniqueName
	if log.V(1) {
		log.Info("GCS destination: ", gcsPath)
	}

	// Copy compressed binary to Google Cloud Storage.
	gsutil := exec.Command("gsutil", "cp", "-Z", path, gcsPath)
	gsutil.Stderr = os.Stderr
	gsutil.Stdout = os.Stdout
	if log.V(1) {
		log.Infof("running: %#v", gsutil.Args)
	}
	if err := gsutil.Run(); err != nil {
		return err
	}

	// From each machine, download binary and symlink it into place.
	dshCmdLine := fmt.Sprintf("gsutil cp %s . && chmod +x %s && ln -sf %s %s",
		gcsPath, uniqueName, uniqueName, binName)
	if err := dshCluster(ctx, outputVar, dshCmdLine); err != nil {
		return err
	}

	// TODO(cuongdo): Garbage-collect old binaries.

	log.Infof("new '%s' binary pushed to cluster", binName)
	return nil
}

// pushCockroach copies the given CockroachDB binary to all nodes and restarts
// the cluster.
func pushCockroach(ctx ccContext, path string) error {
	// It's fine that this fails if CockroachDB is not already running.
	_ = dshSupervisorctl(ctx, tfCockroachIPs, "stop", "cockroach")
	if err := pushBinary(ctx, tfCockroachIPs, path); err != nil {
		return err
	}
	return dshSupervisorctl(ctx, tfCockroachIPs, "start", "cockroach")
}

// downloadCockroach downloads the latest official cockroach binary to all
// CockroachDB nodes and restarts all servers.
func downloadCockroach(ctx ccContext) error {
	cmdLine := `supervisorctl -c supervisor.conf stop cockroach; ` +
		`bash download_binary.sh cockroach/cockroach && ` +
		`supervisorctl -c supervisor.conf start cockroach`
	return dshCluster(ctx, tfCockroachIPs, cmdLine)
}

// pushFile copies the given file to all CockroachDB nodes.
func pushFile(ctx ccContext, path string) error {
	return pushBinary(ctx, tfCockroachIPs, path)
}

func uploadStores(ctx ccContext, gcsDir string) error {
	cmdLine := `supervisorctl -c supervisor.conf stop cockroach; ` +
		`./nodectl upload ` + gcsDir
	err := dshCluster(ctx, tfCockroachIPs, cmdLine)
	if err == nil {
		log.Info("finished uploading stores")
	}
	return err
}

func downloadStores(ctx ccContext, gcsDir string) error {
	cmdLine := `supervisorctl -c supervisor.conf stop cockroach; ` +
		`./nodectl download ` + gcsDir
	err := dshCluster(ctx, tfCockroachIPs, cmdLine)
	if err == nil {
		log.Info("finished downloading stores")
	}
	return err
}

func resizeCluster(ctx ccContext, numNodesStr string) error {
	numNodes, err := strconv.Atoi(numNodesStr)
	if err != nil {
		return err
	}
	extraArgs := []string{
		fmt.Sprintf("-var=num_instances=%d", numNodes),
	}
	return execTerraform(ctx, "apply", nil /*stdout*/, extraArgs)
}

func sqlShell(ctx ccContext) error {
	ip, err := randomTerraformOutputVal(ctx, tfCockroachIPs)
	if err != nil {
		return err
	}

	if _, err := exec.LookPath("cockroach"); err != nil {
		return errors.New("couldn't find 'cockroach' binary in your $PATH")
	}
	cmd := exec.Command("cockroach", "sql", "--host", ip)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	// We don't care about errors, because for an interactive SQL session,
	// any errors should have been displayed already.
	if log.V(1) {
		log.Infof("running SQL shell: %#v", cmd.Args)
	}
	_ = cmd.Run()
	return nil
}

func openAdmin(ctx ccContext) error {
	adminURL, err := randomTerraformOutputVal(ctx, tfAdminURLs)
	if err != nil {
		return err
	}
	var cmd *exec.Cmd
	if _, err := exec.LookPath("open"); err == nil {
		// Open URL on OS X.
		cmd = exec.Command("open", adminURL)
	}
	if cmd == nil {
		return errors.New("couldn't find mechanism for opening URLs (unimplemented on your platform?)")
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if log.V(1) {
		log.Infof("opening admin: %s", cmd.Args)
	}
	return cmd.Run()
}

func verifyUp(ctx ccContext) error {
	if err := checkTerraformEnvironment(ctx); err != nil {
		return err
	}

	// Create a slice of CockroachDB URLs.
	ipAddrVal, err := terraformOutputVal(ctx, tfCockroachIPs)
	if err != nil {
		return err
	}
	ipAddrs := strings.Split(ipAddrVal, ",")
	var urls []string
	for _, ip := range ipAddrs {
		urls = append(urls, cockroachURL(ip, 26257))
	}

	if log.V(2) {
		log.Info("urls = ", urls)
	}

	// Verify that each instance is up by issuing a simple SQL command.
	for _, url := range urls {
		if log.V(1) {
			log.Info("checking ", url)
		}
		db, err := gosql.Open("postgres", url)
		if err != nil {
			return err
		}
		if _, err := db.Exec("SELECT 1"); err != nil {
			return err
		}
	}

	log.Info("all nodes are healthy")

	return nil
}
