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
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
)

// tfOutputField is the name of a Terraform output field, which is used to get
// IP addresses for machines, etc. created by Terraform.
type tfOutputVar string

const (
	// Names of Terraform output fields.
	tfCockroachIPs   tfOutputVar = "cockroach_ips"
	tfAdminURLs      tfOutputVar = "admin_urls"
	tfBlockWriterIPs tfOutputVar = "block_writer_ips"
)

func checkTerraformEnvironment(ctx ccContext) error {
	for _, v := range []string{"GOOGLE_CREDENTIALS", "GOOGLE_PROJECT"} {
		// os.Getenv() must be used here, because Terraform expects these
		// environments to not be prefixed with envutil's "COCKROACH_".
		if os.Getenv(v) == "" {
			return fmt.Errorf("Please set the %s environment variable", v)
		}
	}
	if _, err := exec.LookPath("terraform"); err != nil {
		return err
	}
	return nil
}

// terraformArgs returns the Terraform parameters needed to invoke Terraform
// for the given command line parameters.
func terraformArgs(ctx ccContext, action string) []string {
	// Get the prefix that will be prepended to all resources created by
	// Terraform.
	if ctx.namePrefix == "" {
		panic("--name-prefix cannot be empty")
	}

	// We use the namePrefix as the basename for the Terraform .tfstate file.
	// We also need the absolute path, since we'll need to Chdir later.
	statePath, err := filepath.Abs(ctx.namePrefix + ".tfstate")
	if err != nil {
		panic(err)
	}

	switch action {
	case "output":
		return []string{
			action,
			"-state=" + statePath,
		}
	case "destroy":
		return []string{
			action,
			"-state=" + statePath,
			"--force",
		}
	case "apply":
		if log.V(1) {
			log.Infof("extra Terraform variables: %+v", ctx.vars)
		}
		args := []string{
			action,
			"-var=name_prefix=" + ctx.namePrefix,
			"-state=" + statePath,
		}
		for _, v := range ctx.vars {
			args = append(args, "-var="+v)
		}
		return args
	default:
		panic("unimplemented")
	}
}

// execTerraform runs Terraform with the given action and arguments defined by
// the commandContext and extraArgs.
func execTerraform(ctx ccContext, action string, stdout io.Writer, extraArgs []string) error {
	if err := checkTerraformEnvironment(ctx); err != nil {
		return err
	}

	opts := retry.Options{
		InitialBackoff: time.Second,
		MaxBackoff:     5 * time.Minute,
		MaxRetries:     5,
	}
	var err error
	for r := retry.Start(opts); r.Next(); {
		if err = tryExecTerraform(ctx, action, stdout, extraArgs); err == nil {
			return nil
		}
		log.Errorf("terraform %s failed", action)
	}
	return err
}

func tryExecTerraform(ctx ccContext, action string, stdout io.Writer, extraArgs []string) error {
	var args []string
	args = append(args, terraformArgs(ctx, action)...)
	args = append(args, extraArgs...)
	terraform := exec.Command("terraform", args...)
	terraform.Stderr = os.Stderr
	terraform.Stdout = os.Stdout
	if stdout != nil {
		terraform.Stdout = stdout
	}
	terraform.Dir = ctx.tfdir
	if log.V(1) {
		log.Infof("running terraform (dir = %s): %#v", terraform.Dir, terraform.Args)
	}
	tfErr := terraform.Run()
	return tfErr
}

// terraformOutputVal returns the value for the specified Terraform output
// variable.
func terraformOutputVal(ctx ccContext, varName tfOutputVar) (string, error) {
	var outbuf bytes.Buffer
	if err := execTerraform(ctx, "output", &outbuf, nil); err != nil {
		return "", err
	}
	r := bufio.NewReader(&outbuf)
	for {
		l, err := r.ReadString('\n')
		if err == io.EOF {
			return "", util.Errorf("couldn't find Terraform output variable %s", varName)
		} else if err != nil {
			return "", err
		}
		if err != nil {
			return "", err
		}
		if log.V(1) {
			log.Info("read terraform output line: ", l)
		}

		// Check name of this Terraform output variable.
		l = strings.TrimSpace(l)
		tokens := strings.Split(l, " ")
		if a, e := len(tokens), 3; a != e {
			return "", util.Errorf("output variable line (%s) has %d tokens (expected %d)", l, a, e)
		}
		if tokens[0] == string(varName) {
			return strings.TrimSpace(tokens[2]), nil
		}
	}
}

// randomTerraformOutputVal returns a randomly selected value from the specified
// comma-separated Terraform output variable. For example, if the Terraform
// output variable is "apple,banana,carrot", this method can return any of those
// 3 values.
func randomTerraformOutputVal(ctx ccContext, varName tfOutputVar) (string, error) {
	outputVal, err := terraformOutputVal(ctx, varName)
	if err != nil {
		return "", err
	}
	if log.V(1) {
		log.Infof("got value %s for output var %s", outputVal, varName)
	}
	outputVals := strings.Split(outputVal, ",")
	if log.V(1) {
		log.Infof("output values = %#v", outputVals)
	}
	if len(outputVals) == 0 {
		return "", util.Errorf("couldn't find any values for Terraform output var %s", varName)
	}
	return outputVals[rand.Intn(len(outputVals))], nil
}
