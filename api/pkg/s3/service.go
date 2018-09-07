package s3

import (
	"net/http"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	"github.com/micro/go-micro/client"
	//	"github.com/micro/go-micro/errors"
	"encoding/json"
	"github.com/opensds/go-panda/s3/proto"
	"golang.org/x/net/context"
)

const (
	s3Service = "s3"
)

type APIService struct {
	s3Client  s3.S3Service
}

func NewAPIService(c client.Client) *APIService {
	return &APIService{
		s3Client: s3.NewS3Service(s3Service, c),
	}
}

func IsQuery(request *restful.Request, name string) bool {
	params := request.Request.URL.Query()
	if params == nil {
		return false
	}
	if _, ok := params[name]; ok {
		return true
	}
	return false
}


func (s *APIService) BucketPut(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	acl := request.QueryParameter("acl")
	versioning := request.QueryParameter("versioning")
	website := request.QueryParameter("website")
	cors := request.QueryParameter("cors")
	replication := request.QueryParameter("replication")
	lifecycle := request.QueryParameter("lifecycle")
	
	log.Logf("Received request for bucket details: %s", bucketName)
	ctx := context.Background()
	if IsQuery(request, "acl") {
		//TODO
		res, err := s.s3Client.CreateBucket(ctx, &s3.Bucket{Name: bucketName})
	} else if IsQuery(request, "versioning") {
		//TODO
		res, err := s.s3Client.CreateBucket(ctx, &s3.Bucket{Name: bucketName})
	} else if IsQuery(request, "website") {
		//TODO
		res, err := s.s3Client.CreateBucket(ctx, &s3.Bucket{Name: bucketName})
	} else if IsQuery(request, "cors") {
		//TODO
		res, err := s.s3Client.CreateBucket(ctx, &s3.Bucket{Name: bucketName})
		
	} else if IsQuery(request, "replication") {
		//TODO
		res, err := s.s3Client.CreateBucket(ctx, &s3.Bucket{Name: bucketName})
		
	} else if IsQuery(request, "lifecycle") {
		//TODO
		res, err := s.s3Client.CreateBucket(ctx, &s3.Bucket{Name: bucketName})
		
	} else {
		res, err := s.s3Client.CreateBucket(ctx, &s3.Bucket{Name: bucketName})
		if err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		}
		log.Log("Get object details successfully.")
		response.WriteEntity(res)
	}
}

func (s *APIService) BucketGet(request *restful.Request, response *restful.Response) {

}

func (s *APIService) BucketDelete(request *restful.Request, response *restful.Response) {

}



func (s *APIService) ObjectGet(request *restful.Request, response *restful.Response) {
	objectKey := request.PathParameter("objectKey")
	bucketName := request.PathParameter("bucketName")
	log.Logf("Received request for object details: %s %s", bucketName, objectKey)
	ctx := context.Background()
	res, err := s.s3Client.GetObject(ctx, &s3.Object{ObjectKey: objectKey,BucketName: bucketName})
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Log("Get object details successfully.")
	response.WriteEntity(res)
}
