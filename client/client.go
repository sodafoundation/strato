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

package client

import (
	"log"

	"github.com/opensds/multi-cloud/api/pkg/filters/context"
)

// Client is a struct for exposing some operations of resources.
type Client struct {
	*BackendMgr
	*BucketMgr
	*DataflowMgr

	cfg *Config
}

// Config is a struct that defines some options for calling the Client.
type Config struct {
	Endpoint    string
	AuthOptions AuthOptions
}

// NewClient creates a new Client.
func NewClient(c *Config) *Client {
	// If endpoint field not specified, use the default value localhost.
	if c.Endpoint == "" {
		c.Endpoint = "http://localhost:8089"
		log.Printf("Warnning: OpenSDS multi-cloud endpoint is not specified using the default value(%s)", c.Endpoint)
	}

	var r Receiver
	switch c.AuthOptions.(type) {
	case *NoAuthOptions:
		r = NewReceiver()
	case *KeystoneAuthOptions:
		r = NewKeystoneReciver(c.AuthOptions.(*KeystoneAuthOptions))
	default:
		log.Printf("Warnning: Not support auth options, use default")
		r = NewReceiver()
		c.AuthOptions = NewNoauthOptions(context.DefaultTenantId)
	}

	t := c.AuthOptions.GetTenantID()
	return &Client{
		cfg:         c,
		BackendMgr:  NewBackendMgr(r, c.Endpoint, t),
		BucketMgr:   NewBucketMgr(r, c.Endpoint, t),
		DataflowMgr: NewDataflowMgr(r, c.Endpoint, t),
	}
}
