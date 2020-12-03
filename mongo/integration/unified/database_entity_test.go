package unified

import (
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DatabaseEntity struct {
	*mongo.Database
	originalOptions *options.DatabaseOptions
}

func NewDatabaseEntity(client *ClientEntity, name string, dbOptions *options.DatabaseOptions) *DatabaseEntity {
	return newDatabaseEntity(client.Client, name, dbOptions)
}

func newDatabaseEntity(client *mongo.Client, name string, dbOptions ...*options.DatabaseOptions) *DatabaseEntity {
	merged := options.MergeDatabaseOptions(dbOptions...)
	return &DatabaseEntity{
		Database:        client.Database(name, merged),
		originalOptions: merged,
	}
}

func (d *DatabaseEntity) Clone(dbOptions *options.DatabaseOptions) *DatabaseEntity {
	return newDatabaseEntity(d.Client(), d.Name(), d.originalOptions, dbOptions)
}
