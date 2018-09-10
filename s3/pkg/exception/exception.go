package _exception
import(
	"errors"
	"fmt"
)

type S3Error struct {
	Code     int
	Description  string
}

func (err *S3Error) Error() error {
	s := fmt.Sprintf("{\"code\":\"%d\",\"message\":\"%s\"}",err.Code,err.Description)
	return errors.New(s)
}

var ERR_OK = 200
var NoError = S3Error{Code:ERR_OK} 
var InternalError = S3Error{Code:500,Description:"Internal error. Please retry"} 
var NoSuchBucket = S3Error{Code:404,Description:"The specified bucket does not exists."}
var NoSuchObject = S3Error{Code:404,Description:"The specified bucket does not exists."}
var BucketAlreadyExists = S3Error{Code:409,Description:"The requested bucket name already exists. Bucket namespace is shared by all users in the system. Select a different name and retry."}
