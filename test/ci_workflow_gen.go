/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"
	"text/template"
)

const (
	workflowConfigDir = "../.github/workflows"

	unitTestTemplate = "templates/unit_test.tpl"
	// Only MySQL 8.0 is installable from the Ubuntu 22.04 archive and it is
	// what Vinted runs in production. The MySQL 5.6/5.7 and MariaDB 10.x
	// packages the upstream workflows relied on are no longer available on
	// any GitHub-hosted runner.
	unitTestDatabases = "mysql80"

	clusterTestTemplate = "templates/cluster_endtoend_test.tpl"
)

var (
	clusterList = []string{
		"11",
		"12",
		"13",
		"14",
		"15",
		"16",
		"17",
		"18",
		"19",
		"20",
		"21",
		"22",
		"23",
		"24",
		"26",
		"vreplication_basic",
		"vreplication_multicell",
		"vreplication_cellalias",
		"vstream_failover",
		"vreplication_v2",
		"vstream_stoponreshard_true",
		"vstream_stoponreshard_false",
		"onlineddl_ghost",
		"onlineddl_vrepl",
		"onlineddl_vrepl_stress",
		"onlineddl_vrepl_suite",
		"vreplication_migrate",
		"onlineddl_revert",
		"onlineddl_declarative",
		"onlineddl_singleton",
		"tabletmanager_throttler",
		"tabletmanager_throttler_custom_config",
		"tabletmanager_tablegc",
		"vtorc",
		"vtgate_buffer",
		"vtgate_concurrentdml",
		"vtgate_gen4",
		"vtgate_readafterwrite",
		"vtgate_reservedconn",
		"vtgate_schema",
		"vtgate_topo",
		"vtgate_transaction",
		"vtgate_unsharded",
		"vtgate_vindex",
		"vtgate_vschema",
		"xb_recovery",
		"resharding",
		"resharding_bytes",
		"mysql80",
	}
	// Clusters that run on every push/pull request. All other clusters are
	// generated with a manual (workflow_dispatch) trigger only: they were
	// written for Ubuntu 18.04 + MySQL 5.7 and have not been verified against
	// the MySQL 8.0 that current GitHub runners provide.
	clustersOnPullRequest = []string{
		"vreplication_basic",
		"vstream_failover",
		"vstream_stoponreshard_true",
		"vstream_stoponreshard_false",
	}
	// TODO: currently some percona tools including xtrabackup are installed on all clusters, we can possibly optimize
	// this by only installing them in the required clusters
	clustersRequiringXtraBackup = clusterList
	clustersRequiringMakeTools  = []string{
		"18",
		"24",
	}
)

type unitTest struct {
	Name, Platform string
}

type clusterTest struct {
	Name, Shard                  string
	MakeTools, InstallXtraBackup bool
	OnPullRequest                bool
}

func mergeBlankLines(buf *bytes.Buffer) string {
	var out []string
	in := strings.Split(buf.String(), "\n")
	lastWasBlank := false
	for _, line := range in {
		if strings.TrimSpace(line) == "" {
			if lastWasBlank {
				continue
			}
			lastWasBlank = true
		} else {
			lastWasBlank = false
		}

		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

func main() {
	generateUnitTestWorkflows()
	generateClusterWorkflows()
}

func canonnizeList(list []string) []string {
	var output []string
	for _, item := range list {
		item = strings.TrimSpace(item)
		if item != "" {
			output = append(output, item)
		}
	}
	return output
}
func parseList(csvList string) []string {
	var list []string
	for _, item := range strings.Split(csvList, ",") {
		list = append(list, strings.TrimSpace(item))
	}
	return list
}

func generateClusterWorkflows() {
	clusters := canonnizeList(clusterList)
	for _, cluster := range clusters {
		test := &clusterTest{
			Name:  fmt.Sprintf("Cluster (%s)", cluster),
			Shard: cluster,
		}
		makeToolClusters := canonnizeList(clustersRequiringMakeTools)
		for _, makeToolCluster := range makeToolClusters {
			if makeToolCluster == cluster {
				test.MakeTools = true
				break
			}
		}
		xtraBackupClusters := canonnizeList(clustersRequiringXtraBackup)
		for _, xtraBackupCluster := range xtraBackupClusters {
			if xtraBackupCluster == cluster {
				test.InstallXtraBackup = true
				break
			}
		}
		pullRequestClusters := canonnizeList(clustersOnPullRequest)
		for _, pullRequestCluster := range pullRequestClusters {
			if pullRequestCluster == cluster {
				test.OnPullRequest = true
				break
			}
		}

		path := fmt.Sprintf("%s/cluster_endtoend_%s.yml", workflowConfigDir, cluster)
		generateWorkflowFile(clusterTestTemplate, path, test)
	}
}

func generateUnitTestWorkflows() {
	platforms := parseList(unitTestDatabases)
	for _, platform := range platforms {
		test := &unitTest{
			Name:     fmt.Sprintf("Unit Test (%s)", platform),
			Platform: platform,
		}
		path := fmt.Sprintf("%s/unit_test_%s.yml", workflowConfigDir, platform)
		generateWorkflowFile(unitTestTemplate, path, test)
	}
}

func generateWorkflowFile(templateFile, path string, test interface{}) {
	tpl, err := template.ParseFiles(templateFile)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	buf := &bytes.Buffer{}
	err = tpl.Execute(buf, test)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	f, err := os.Create(path)
	if err != nil {
		log.Println("Error creating file: ", err)
		return
	}
	f.WriteString("# DO NOT MODIFY: THIS FILE IS GENERATED USING \"make generate_ci_workflows\"\n\n")
	f.WriteString(mergeBlankLines(buf))
	fmt.Printf("Generated %s\n", path)

}
