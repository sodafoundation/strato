package queryrunner

import (
	"context"

	"github.com/opensds/multi-cloud/metadata/pkg/db"
	"github.com/opensds/multi-cloud/metadata/pkg/model"
	bson2 "go.mongodb.org/mongo-driver/bson"
)

func ExecuteQuery(ctx context.Context, query []bson2.D) ([]*model.MetaBackend, error) {
	return db.DbAdapter.ListMetadata(ctx, query)
}
