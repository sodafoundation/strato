// Copyright (c) 2019 Huawei Technologies Co., Ltd. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*
This module implements a entry into the OpenSDS service.

*/

package cli

import (
	"encoding/json"
	"os"

	backend "github.com/opensds/multi-cloud/backend/proto"
	"github.com/spf13/cobra"
)

var (
	access   string
	security string
)

var backendCommand = &cobra.Command{
	Use:   "backend",
	Short: "manage backends",
	Run:   backendAction,
}

var backendCreateCommand = &cobra.Command{
	Use:   "create <backend info>",
	Short: "create a backend",
	Run:   backendCreateAction,
}

var backendDeleteCommand = &cobra.Command{
	Use:   "delete <id>",
	Short: "delete a backend",
	Run:   backendDeleteAction,
}

var backendShowCommand = &cobra.Command{
	Use:   "show <id>",
	Short: "show a backend",
	Run:   backendShowAction,
}

var backendListCommand = &cobra.Command{
	Use:   "list",
	Short: "list all backends",
	Run:   backendListAction,
}

var backendUpdateCommand = &cobra.Command{
	Use:   "update <id>",
	Short: "update a backend",
	Run:   backendUpdateAction,
}

var typeCommand = &cobra.Command{
	Use:   "type",
	Short: "manage types",
	Run:   backendAction,
}

var typeListCommand = &cobra.Command{
	Use:   "list",
	Short: "list all types",
	Run:   typeListAction,
}

func init() {
	backendCommand.AddCommand(backendCreateCommand)
	backendCommand.AddCommand(backendDeleteCommand)
	backendCommand.AddCommand(backendShowCommand)
	backendCommand.AddCommand(backendListCommand)

	backendCommand.AddCommand(backendUpdateCommand)
	backendUpdateCommand.Flags().StringVarP(&access, "access", "a", "", "the access of updated backend")
	backendUpdateCommand.Flags().StringVarP(&security, "security", "s", "", "the security of updated backend")

	typeCommand.AddCommand(typeListCommand)
}

func backendAction(cmd *cobra.Command, args []string) {
	cmd.Usage()
	os.Exit(1)
}

func backendCreateAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 1)
	backend := &backend.BackendDetail{}
	if err := json.Unmarshal([]byte(args[0]), backend); err != nil {
		Errorln(err)
		cmd.Usage()
		os.Exit(1)
	}

	resp, err := client.CreateBackend(backend)
	if err != nil {

		Fatalln(HTTPErrStrip(err))
	}
	keys := KeyList{"Id", "TenantId", "UserId", "Name", "Type", "Region",
		"Endpoint", "BucketName", "Access", "Security"}
	PrintDict(resp, keys, FormatterList{})
}

func backendDeleteAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 1)
	err := client.DeleteBackend(args[0])
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}
}

func backendShowAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 1)
	resp, err := client.GetBackend(args[0])
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}
	keys := KeyList{"Id", "TenantId", "UserId", "Name", "Type", "Region",
		"Endpoint", "BucketName", "Access", "Security"}

	PrintDict(resp, keys, FormatterList{})
}

func backendListAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 0)
	resp, err := client.ListBackends()
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}
	keys := KeyList{"Id", "TenantId", "UserId", "Name", "Type", "Region",
		"Endpoint", "BucketName", "Access", "Security"}
	PrintList(resp.Backends, keys, FormatterList{})
}

func backendUpdateAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 1)
	backend := &backend.UpdateBackendRequest{
		Id:       args[0],
		Access:   access,
		Security: security,
	}

	resp, err := client.UpdateBackend(backend)
	if err != nil {

		Fatalln(HTTPErrStrip(err))
	}
	keys := KeyList{"Id", "TenantId", "UserId", "Name", "Type", "Region",
		"Endpoint", "BucketName", "Access", "Security"}
	PrintDict(resp, keys, FormatterList{})
}

func typeListAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 0)
	resp, err := client.ListTypes()
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}
	keys := KeyList{"Name", "Description"}
	PrintList(resp.Types, keys, FormatterList{})
}
