// Copyright 2019 The OpenSDS Authors.
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

package sony

import (
	"context"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/error"
	dscommon "github.com/opensds/multi-cloud/s3/pkg/datastore/common"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
	"github.com/webrtcn/s3client"
	"io"
)

type SonyAdapter struct {
	backend *backendpb.BackendDetail
	session *s3client.Client
}

func (ad *SonyAdapter) Put(ctx context.Context, stream io.Reader, object *pb.Object) (result dscommon.PutResult, err error) {
	return result, nil
}

func (ad *SonyAdapter) Get(ctx context.Context, object *pb.Object, start int64, end int64) (io.ReadCloser, error) {
	return nil, ErrNotImplemented
}

func (ad *SonyAdapter) Delete(ctx context.Context, object *pb.DeleteObjectInput) error {
	return nil
}

func (ad *SonyAdapter) Copy(ctx context.Context, stream io.Reader, target *pb.Object) (result dscommon.PutResult, err error) {
	log.Errorf("copy[Sony S3] is not supported.")
	err = ErrInternalError
	return
}

func (ad *SonyAdapter) ChangeStorageClass(ctx context.Context, object *pb.Object, newClass *string) error {
	log.Errorf("change storage class[Sony S3] is not supported.")
	return ErrInternalError
}

func (ad *SonyAdapter) InitMultipartUpload(ctx context.Context, object *pb.Object) (*pb.MultipartUpload, error) {
	return nil, ErrNotImplemented
}

func (ad *SonyAdapter) UploadPart(ctx context.Context, stream io.Reader, multipartUpload *pb.MultipartUpload,
	partNumber int64, upBytes int64) (*model.UploadPartResult, error) {
	return nil, ErrNotImplemented
}

func (ad *SonyAdapter) CompleteMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload,
	completeUpload *model.CompleteMultipartUpload) (*model.CompleteMultipartUploadResult, error) {
	return nil, ErrNotImplemented
}

func (ad *SonyAdapter) AbortMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload) error {
	return nil
}

func (ad *SonyAdapter) ListParts(ctx context.Context, multipartUpload *pb.ListParts) (*model.ListPartsOutput, error) {
	return nil, ErrNotImplemented
}

func (ad *SonyAdapter) BackendCheck(ctx context.Context, backendDetail *pb.BackendDetailS3) error {
	return ErrNotImplemented
}

func (ad *SonyAdapter) Restore(ctx context.Context, inp *pb.Restore) error {
	return ErrNotImplemented
}

func (ad *SonyAdapter) Close() error {
	// TODO
	return nil
}
