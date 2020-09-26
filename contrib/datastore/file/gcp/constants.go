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

package gcp

const (

	// The service tier of the fileStore instance in GCP.
	// Supported service tiers at GCP are:
	//   "TIER_UNSPECIFIED" - Not set.
	//   "STANDARD" - STANDARD tier.
	//   "PREMIUM" - PREMIUM tier.
	Tier = "Tier"

	// The default Network name for Google Compute Engine.
	DefaultNetwork = "default"

	// The IPv4 internet protocol mode for GCP.
	InternetProtocolModeIpv4 = "MODE_IPV4"

	// The Unique name of the instance resource in GCP.
	InstanceResourceName = "InstanceResourceName"

	// The time that the file system was created in GCP.
	CreationTimeAtBackend = "CreationTimeAtBackend"

	// Server-specified ETag in GCP.
	Etag = "Etag"

	// Networks: VPC networks to which the instance is connected, in GCP.
	Networks = "Networks"

	// FileStore Instance State, in GCP.
	State = "State"

	// FileStore Instance Status about its State, in GCP.
	StatusMessage = "StatusMessage"
)
