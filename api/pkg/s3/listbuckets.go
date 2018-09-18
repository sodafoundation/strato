package s3

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	"net/http"
	//	"github.com/micro/go-micro/errors"
	"encoding/xml"
	"github.com/opensds/go-panda/s3/proto"
	"golang.org/x/net/context"
	"strings"
	"time"
)

func parseListBuckets(list *s3.ListBucketsResponse) *string {
	result := []string{}
	if list == nil || list.Buckets == nil {
		return nil
	}
	temp := ListAllMyBucketsResult{}
	//default xmlns
	temp.Xmlns = Xmlns
	buckets := []Bucket{}
	for _, value := range list.Buckets {
		creationDate := time.Unix(value.CreationDate, 0).Format(time.RFC3339)
		buckets = append(buckets, Bucket{Name: value.Name, CreationDate: creationDate})
	}
	temp.Buckets = buckets

	data, err := xml.MarshalIndent(temp, "", "")
	if err != nil {
		log.Logf("Parse ListBuckets error: %v", err)
		return nil
	}
	result = append(result, xml.Header)
	result = append(result, string(data))
	str := strings.Join(result, "")
	return &str
}

func (s *APIService) ListBuckets(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	//TODO owner
	owner := "test"
	log.Logf("Received request for all buckets")
	res, err := s.s3Client.ListBuckets(ctx, &s3.BaseRequest{Id: owner})
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	realRes := parseListBuckets(res)

	log.Logf("Get List of buckets successfully:%v\n", realRes)

	response.WriteEntity(realRes)

}
