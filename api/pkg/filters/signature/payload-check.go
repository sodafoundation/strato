package signature

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"io/ioutil"

	"github.com/emicklei/go-restful"
	. "github.com/opensds/multi-cloud/s3/error"
	log "github.com/sirupsen/logrus"
)

func PayloadCheck(request *restful.Request, response *restful.Response) error {
	r := request.Request
	payload, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorf("Read body failed: %v\n", err)
		return ErrInternalError
	}
	// Verify Content-Md5, if payload is set.
	inputMd5Sum := r.Header.Get("Content-Md5")
	if inputMd5Sum != "" {
		// check if it is valid
		_, err := base64.StdEncoding.DecodeString(inputMd5Sum)
		if err != nil {
			log.Errorln("ErrInvalidDigest")
			return ErrInvalidDigest
		}
		if inputMd5Sum != base64.StdEncoding.EncodeToString(sumMD5(payload)) {
			log.Errorln("ErrBadDigest")
			return ErrBadDigest
		}
	}
	// Verify Content-Sha256, this is normally used for authv4.
	inputSha256Sum := r.Header.Get("X-Amz-Content-Sha256")
	if inputSha256Sum != "" && inputSha256Sum != UnsignedPayload {
		if r.Header.Get("X-Amz-Content-Sha256") != hex.EncodeToString(sum256(payload)) {
			log.Errorln("hashedPayload:", hex.EncodeToString(sum256(payload)), ", received:",
				r.Header.Get("X-Amz-Content-Sha256"))
			return ErrContentSHA256Mismatch
		}
	}

	// Populate back the payload.
	r.Body = ioutil.NopCloser(bytes.NewReader(payload))
	return nil
}
