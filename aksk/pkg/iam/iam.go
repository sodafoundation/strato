package iam

import (
	"context"
	"fmt"
	"github.com/opensds/multi-cloud/aksk/pkg/iam/driver/keystone"
	"github.com/opensds/multi-cloud/aksk/pkg/model"
	"github.com/opensds/multi-cloud/aksk/pkg/utils/config"
	pb "github.com/opensds/multi-cloud/aksk/proto"
)

type IAMAuthenticator interface {
	// AkSk
	CreateAkSk(ctx context.Context, AkSk *model.AkSk) (*model.AkSk, error)
	DeleteAkSk(ctx context.Context, in *pb.DeleteAkSkRequest) error
	GetAkSk(ctx context.Context, in *pb.GetAkSkRequest) (*model.AkSk, error)
	ListAkSk(ctx context.Context, limit, offset int, query interface{}) ([]*model.AkSk, error)
	Close()
}

var CredStore IAMAuthenticator

func Init(iam *config.CredentialStore) {
	switch iam.Driver {
	case "keystone":
		Iam := keystone.Init(iam.Host)
		fmt.Printf("Initializing Keystone!\n", Iam)
		return
	default:
		fmt.Printf("Can't find Credentials driver %s!\n", iam.Driver)
	}
}

func Exit() {
	CredStore.Close()
}
