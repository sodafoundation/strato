package s3

import (
	"encoding/xml"
	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	"net/http"
	//	"github.com/micro/go-micro/errors"
	"github.com/opensds/go-panda/s3/proto"
	"golang.org/x/net/context"
	"time"
)

func (s *APIService) BucketPut(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")

	log.Logf("Received request for create bucket: %s", bucketName)
	ctx := context.Background()

	bucket := s3.Bucket{Name: bucketName}
	body := ReadBody(request)
	//TODO owner
	owner := "test"
	ownerDisplayName := "test"
	bucket.Owner = owner
	bucket.OwnerDisplayName = ownerDisplayName
	bucket.CreationDate = time.Now().Unix()
	log.Logf("Create bucket body: %s", string(body))
	if body != nil {
		createBucketConf := CreateBucketConfiguration{}
		err := xml.Unmarshal(body, &createBucketConf)
		if err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		} else {
			bucket.Backend = createBucketConf.LocationConstraint
		}
	}

	res, err := s.s3Client.CreateBucket(ctx, &bucket)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	log.Log("Create bucket successfully.")
	response.WriteEntity(res)

}
