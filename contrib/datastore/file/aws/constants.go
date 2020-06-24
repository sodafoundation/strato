// Copyright 2020 The SODA Authors.
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

package aws

const (
	// The file system Name tag, Amazon EFS return.
	FileSystemName = "Name"

	// The ID of the file system, assigned by Amazon EFS.
	FileSystemId = "FileSystemId"

	// The AWS account that created the file system.
	OwnerId = "OwnerId"

	// FileSystem Size Object in AWS.
	FileSystemSize = "FileSystemSize"

	// The ID of an AWS Key Management Service (AWS KMS) customer master key (CMK)
	// that was used to protect the encrypted file system.
	KmsKeyId = "KmsKeyId"

	// The throughput mode for a file system in AWS.
	ThroughputMode = "ThroughputMode"

	// The throughput for a file system provisioning in AWS.
	ProvisionedThroughputInMibps = "ProvisionedThroughputInMibps"

	// The performance mode of the file system in AWS.
	PerformanceMode = "PerformanceMode"

	// The opaque string specified for AWS FileSystem.
	CreationToken = "CreationToken"

	// The time that the file system was created in AWS.
	CreationTimeAtBackend = "CreationTimeAtBackend"

	// The lifecycle phase of the file system in AWS.
	LifeCycleState = "LifeCycleState"

	// The current number of mount targets that the file system has, in AWS.
	NumberOfMountTargets = "NumberOfMountTargets"
)
