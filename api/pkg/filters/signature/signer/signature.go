// Copyright 2019 The OpenSDS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Package signer implements signing and signature validation for opensds multi-cloud signer.
//
// Provides request signing for request that need to be signed with the Signature.
// Provides signature validation for request.
//
package signer

import (
	"net/http"
	"net/url"
	"strings"

	c "github.com/opensds/multi-cloud/api/pkg/context"
	. "github.com/opensds/multi-cloud/api/pkg/filters/signature"
	"github.com/opensds/multi-cloud/api/pkg/filters/signature/credentials"
	"github.com/opensds/multi-cloud/api/pkg/s3"
	s3error "github.com/opensds/multi-cloud/s3/error"

	"github.com/emicklei/go-restful"
	log "github.com/sirupsen/logrus"
)

type SignatureBase interface {
	Filter(req *restful.Request, resp *restful.Response, chain *restful.FilterChain)
}

type Signature struct {
	Service string
	Region  string
	Request *http.Request
	Body    string
	Query   url.Values

	SignedHeaderValues http.Header

	credValues       credentials.Value
	requestDateTime  string
	requestDate      string
	requestPayload   string
	signedHeaders    string
	canonicalHeaders string
	canonicalString  string
	credentialString string
	stringToSign     string
	signature        string
	authorization    string
}

func NewSignature() SignatureBase {
	return &Signature{}
}

func FilterFactory() restful.FilterFunction {
	sign := NewSignature()
	return sign.Filter
}

// Signature Authorization Filter to validate the Request Signature
// Authorization: algorithm Credential=accesskeyID/credential scope, SignedHeaders=SignedHeaders, Signature=signature
// credential scope <requestDate>/<region>/<service>/sign_request
func (sign *Signature) Filter(req *restful.Request, resp *restful.Response, chain *restful.FilterChain) {
	var cred credentials.Value
	var err error
	authType := GetRequestAuthType(req.Request)
	switch authType {
	case AuthTypeSignedV4, AuthTypePresignedV4,
		AuthTypePresignedV2, AuthTypeSignedV2:
		log.Infof("[%s], AuthTypeSigned:%v", req.Request.URL, authType)
		if cred, err = IsReqAuthenticated(req.Request); err != nil {
			log.Errorf("[%s] reject: IsReqAuthenticated return false, err:%v\n", req.Request.URL, err)
			s3.WriteErrorResponse(resp, req, err)
			return
		}
		// TODO: check bucket policy
	case AuthTypeAnonymous:
		log.Debugf("[%s], AuthTypeSigned:%v", req.Request.URL, authType)
		if !isValidAnonymousRequest(req.Request) {
			log.Errorf("[%s] reject: Authenticated required.\n", req.Request.URL)
			s3.WriteErrorResponse(resp, req, s3error.ErrAccessDenied)
			return
		}
	case AuthTypePostPolicy:
		// in this case, authentication infomation is in the request body, it will be checked in s3 service
		log.Infof("AuthTypePostPolicy: no need auth check here")
	default:
		log.Errorf("[%s] reject: AuthTypeUnknown\n", req.Request.URL)
		s3.WriteErrorResponse(resp, req, s3error.ErrSignatureVersionNotSupported)
		return
	}

	ctx := req.Attribute(c.KContext).(*c.Context)
	ctx.TenantId = cred.TenantID
	ctx.UserId = cred.UserID

	chain.ProcessFilter(req, resp)
}

// Only list buckets and create bucket can be anonymous.
func isValidAnonymousRequest(req *http.Request) bool {
	method := req.Method
	path := req.URL.Path
	if method == "GET" && path == "/" {
		// This is for list buckets
		return false
	} else if method == "PUT" {
		if strings.IndexAny(path[1:], "/") == -1 {
			// This is for create bucket, URL is suposed to be /bucket-name
			return false
		}
	}

	return true
}
