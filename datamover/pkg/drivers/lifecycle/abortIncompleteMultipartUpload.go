package lifecycle

import (
	"errors"

	"github.com/opensds/multi-cloud/datamover/proto"
	log "github.com/sirupsen/logrus"
)

func doAbortUpload(acReq *datamover.LifecycleActionRequest) error {
	log.Error("not implemented")
	return errors.New("not implemented")
}
