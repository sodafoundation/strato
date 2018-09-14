package s3

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/client"
	//	"github.com/micro/go-micro/errors"
	"github.com/opensds/go-panda/s3/proto"
	"io"
	"io/ioutil"
)

const (
	s3Service = "s3"
)

type APIService struct {
	s3Client s3.S3Service
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

func HasHeader(request *restful.Request, name string) bool {
	param := request.HeaderParameter(name)
	if param == "" {
		return false
	}
	return true
}

func ReadBody(r *restful.Request) []byte {
	var reader io.Reader = r.Request.Body
	b, e := ioutil.ReadAll(reader)
	if e != nil {
		return nil
	}
<<<<<<< HEAD
	return b
=======

	//For debug -- begin
	log.Logf("Get policy reponse:%v",res)
	log.Logf("res.ErrCode:%d",res.ErrCode)
	jsons, errs := json.Marshal(res)
	if errs != nil {
		log.Logf(errs.Error())
	} else {
		log.Logf("res: %s.\n", jsons)
	}
	//For debug -- end

	log.Log("Get policy details successfully.")
	response.WriteEntity(res)

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
>>>>>>> 284ae9507f6388fd923acb05662914dd42d19d27
}
