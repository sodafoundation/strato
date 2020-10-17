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

package sfs

const (
	VpcID = "VpcID"

	// The UUID of the file share in Hw Cloud Backend
	ShareID = "ShareID"

	// The type of the volume in Hw Cloud Backend
	VolumeType = "VolumeType"

	// Timestamp when the file share was created in Hw Cloud Backend
	CreatedAt = "CreatedAt"

	// Specifies the mount location
	ExportLocation = "ExportLocation"

	//Lists the mount locations
	ExportLocations = "ExportLocations"

	// The host name of the file share in Hw Cloud Backend
	Host = "Host"

	// The visibility of the file share
	IsPublic = "IsPublic"

	// File Share links for pagination in Hw Cloud Backend
	Links = "Links"

	// The UUID of the project to which the file share belongs to in Hw Cloud Backend
	ProjectID = "ProjectID"

	// The UUID of the file share network in Hw Cloud Backend
	ShareNetworkID = "ShareNetworkID"

	// The UUID of the file share type in Hw Cloud Backend
	ShareType = "ShareType"

	// UUID of the snapshot from which to create the file share in Hw Cloud Backend
	SnapshotID = "SnapshotID"

	// The encryption key ID
	KmsKeyId = "KmsKeyId"

	// The encryption key alias
	KmsKeyName = "KmsKeyName"

	// The tenant domain ID
	DomainId = "DomainId"

	// The encryption key ID in Hw Cloud Backend
	SfsCryptKeyId = "#sfs_crypt_key_id"

	// The encryption key alias in Hw Cloud Backend
	SfsCryptAlias = "#sfs_crypt_alias"

	// The tenant domain ID in Hw Cloud Backend
	SfsCryptDomainId = "#sfs_crypt_domain_id"

	// The key to specify FS Type
	HwSFSType = "HwSFSType"

	// The SFS type supported in Hw Cloud Backend
	HwSFS = "SFS"

	// The SFS "available" state in Hw Cloud Backend
	AvailableState = "available"

	// The access rule "cert" type in Hw Cloud Backend
	CertAccessType = "cert"

	// The access level "rw" to the file share in Hw Cloud Backend
	RWAccessLevel = "rw"
)
