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
	"os"

	c "github.com/opensds/multi-cloud/client"
	s3 "github.com/opensds/multi-cloud/s3/pkg/model"
	"github.com/spf13/cobra"
)

var (
	xmlns              string
	locationconstraint string
)

// S3BaseResp struct
type S3BaseResp struct {
	HTTPStatusCode string
	Message        string
}

var bucketCommand = &cobra.Command{
	Use:   "bucket",
	Short: "manage buckets",
	Run:   bucketAction,
}

var bucketCreateCommand = &cobra.Command{
	Use:   "create <bucket name>",
	Short: "create a bucket",
	Run:   bucketCreateAction,
}

var bucketDeleteCommand = &cobra.Command{
	Use:   "delete <bucket name>",
	Short: "delete a bucket",
	Run:   bucketDeleteAction,
}

var bucketListCommand = &cobra.Command{
	Use:   "list <bucket name>",
	Short: "list all buckets",
	Run:   bucketListAction,
}

var objectCommand = &cobra.Command{
	Use:   "object",
	Short: "manage objects",
	Run:   objectAction,
}

var objectListCommand = &cobra.Command{
	Use:   "list <bucket name>",
	Short: "list all objects in a bucket",
	Run:   objectListAction,
}

var objectUploadCommand = &cobra.Command{
	Use:   "upload <bucket name> <object key> <object>",
	Short: "upload object",
	Run:   objectUploadAction,
}

var objectDownloadCommand = &cobra.Command{
	Use:   "download <bucket name> <object>",
	Short: "download object",
	Run:   objectDownloadAction,
}

var objectDeleteCommand = &cobra.Command{
	Use:   "delete <bucket name> <object>",
	Short: "delete object",
	Run:   objectDeleteAction,
}

func init() {
	bucketCommand.AddCommand(bucketCreateCommand)
	bucketCreateCommand.Flags().StringVarP(&xmlns, "xmlns", "x", "", "the xmlns of updated bucket")
	bucketCreateCommand.Flags().StringVarP(&locationconstraint, "locationconstraint", "l", "", "the location constraint of updated bucket")

	bucketCommand.AddCommand(bucketDeleteCommand)
	bucketCommand.AddCommand(bucketListCommand)

	objectCommand.AddCommand(objectListCommand)
	objectCommand.AddCommand(objectUploadCommand)
	objectCommand.AddCommand(objectDownloadCommand)
	objectCommand.AddCommand(objectDeleteCommand)
}

func bucketAction(cmd *cobra.Command, args []string) {
	cmd.Usage()
	os.Exit(1)
}

func objectAction(cmd *cobra.Command, args []string) {
	cmd.Usage()
	os.Exit(1)
}

// PrintS3BaseResp implementation
func PrintS3BaseResp(resp *c.CBaseResponse) {
	if nil == resp {
		return
	}

	var S3Resp S3BaseResp
	var keys KeyList

	if (nil != resp.CErrorCode) && ("" != resp.CErrorCode.Value) {
		S3Resp.HTTPStatusCode = resp.CErrorCode.Value
		keys = append(keys, "HTTPStatusCode")
	}

	if (nil != resp.CMsg) && ("" != resp.CMsg.Value) {
		S3Resp.Message = resp.CMsg.Value
		keys = append(keys, "Message")
	}

	if len(keys) != 0 {
		PrintDict(S3Resp, keys, FormatterList{})
	}
}

func bucketCreateAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 1)
	bucket := &s3.CreateBucketConfiguration{
		Xmlns:              xmlns,
		LocationConstraint: locationconstraint,
	}

	resp, err := client.CreateBucket(args[0], bucket)
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}

	PrintS3BaseResp(resp)
}

func bucketDeleteAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 1)

	resp, err := client.DeleteBucket(args[0])
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}

	PrintS3BaseResp(resp)
}

func bucketListAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 0)

	resp, err := client.ListBuckets()
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}

	keys := KeyList{"Name", "CreationDate", "LocationConstraint"}
	PrintList(resp, keys, FormatterList{})
}

func objectListAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 1)

	resp, err := client.ListObjects(args[0])
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}

	keys := KeyList{"ObjectKey", "BucketName", "Size", "Backend"}
	PrintList(resp, keys, FormatterList{})
}

func objectUploadAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 3)

	// <bucket name> <object key> <object>
	resp, err := client.UploadObject(args[0], args[1], args[2])
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}

	PrintS3BaseResp(resp)
}

func objectDownloadAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 2)

	err := client.DownloadObject(args[0], args[1])
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}
}

func objectDeleteAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 2)

	resp, err := client.DeleteObject(args[0], args[1])
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}

	PrintS3BaseResp(resp)
}
