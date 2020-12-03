package unified

import (
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type CollectionEntity struct {
	*mongo.Collection
	originalOptions *options.CollectionOptions
}

func NewCollectionEntity(db *DatabaseEntity, name string, collOptions *options.CollectionOptions) *CollectionEntity {
	return newCollectionEntity(db.Database, name, collOptions)
}

func newCollectionEntity(db *mongo.Database, name string, collOptions ...*options.CollectionOptions) *CollectionEntity {
	merged := options.MergeCollectionOptions(collOptions...)
	return &CollectionEntity{
		Collection:      db.Collection(name, merged),
		originalOptions: merged,
	}
}

func (c *CollectionEntity) Clone(collOpts *options.CollectionOptions) *CollectionEntity {
	return newCollectionEntity(c.Database(), c.Name(), c.originalOptions, collOpts)
}
