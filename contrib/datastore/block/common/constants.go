// Copyright 2020 The OpenSDS Authors.
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
package common

const (
	GB_FACTOR = 1024 * 1024 * 1024

	// Information about the volume attachments.
	Attachments = "Attachments"

	// The time that the volume was created.
	CreationTimeAtBackend = "CreationTimeAtBackend"

	// Indicates whether the volume was created using fast snapshot restore.
	FastRestored = "FastRestored"

	// Information about the volume iops.
	Iops = "Iops"

	// The ID of Key Management Service (KMS) customer master key (CMK)
	// that was used to protect the encrypted volume.
	KmsKeyId = "KmsKeyId"

	// The Amazon Resource Name (ARN) of the Outpost.
	OutpostArn = "OutpostArn"

	// The ID of the volume.
	VolumeId = "VolumeId"

	// The type of the volume.
	VolumeType = "VolumeType"

	// The type of the volume.
	Progress = "Progress"

	// The modification completion or failure time at backend .
	StartTimeAtBackend = "StartTimeAtBackend"

	// The modification completion or failure time at backend.
	EndTimeAtBackend = "EndTimeAtBackend"

	// A status message about the modification progress or failure.
	StatusMessage = "StatusMessage"
)
