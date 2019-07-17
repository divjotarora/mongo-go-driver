package integration

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/internal/testutil/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"io/ioutil"
	"path"
	"strings"
	"testing"
)

// findJSONFilesInDir finds the JSON files in a directory.
func findJSONFilesInDir(t *testing.T, dir string) []string {
	t.Helper()
	files := make([]string, 0)

	entries, err := ioutil.ReadDir(dir)
	assert.Nil(t, err, "unable to read json file: %v", err)

	for _, entry := range entries {
		if entry.IsDir() || path.Ext(entry.Name()) != ".json" {
			continue
		}

		files = append(files, entry.Name())
	}

	return files
}

func killSessions(mt *mtest.T) {
	mt.Helper()
	err := mt.Client.Database("admin").RunCommand(mtest.Background, bson.D{
		{"killAllSessions", bson.A{}},
	}, options.RunCmd().SetReadPreference(mtest.PrimaryRp)).Err()
	assert.Nil(mt, err, "unable to killAllSessions: %v", err)
}

func sanitizeCollectionName(kind string, name string) string {
	// Collections can't have "$" in their names, so we substitute it with "%".
	name = strings.Replace(name, "$", "%", -1)

	// Namespaces can only have 120 bytes max.
	if len(kind+"."+name) >= 119 {
		name = name[:119-len(kind+".")]
	}

	return name
}

func distinctWorkaround(mt *mtest.T, db, coll string) {
	mt.Helper()
	opts := options.Client().ApplyURI(mt.ConnString())
	hosts := opts.Hosts
	for _, host := range hosts {
		client, err := mongo.Connect(mtest.Background, opts.SetHosts([]string{host}))
		assert.Nil(mt, err, "error connecting sharded client: %v", err)
		defer func() { _ = client.Disconnect(mtest.Background) }()
		_, err = client.Database(db).Collection(coll).Distinct(mtest.Background, "x", bson.D{})
		assert.Nil(mt, err, "error running Distinct: %v", err)
	}
}

func createClientOptions(optsMap map[string]interface{}) *options.ClientOptions {
	opts := options.Client()
	for name, opt := range optsMap {
		switch name {
		case "retryWrites":
			opts.SetRetryWrites(opt.(bool))
		case "w":
			var wc *writeconcern.WriteConcern
			switch opt.(type) {
			case float64:
				wc = writeconcern.New(writeconcern.W(int(opt.(float64))))
			case string:
				wc = writeconcern.New(writeconcern.WMajority())
			}
			opts.SetWriteConcern(wc)
		case "readConcernLevel":
			rc := readconcern.New(readconcern.Level(opt.(string)))
			opts.SetReadConcern(rc)
		case "readPreference":
			rp := readPrefFromString(opt.(string))
			opts.SetReadPreference(rp)
		}
	}
}

func readPrefFromString(s string) *readpref.ReadPref {
	switch strings.ToLower(s) {
	case "primary":
		return readpref.Primary()
	case "primarypreferred":
		return readpref.PrimaryPreferred()
	case "secondary":
		return readpref.Secondary()
	case "secondarypreferred":
		return readpref.SecondaryPreferred()
	case "nearest":
		return readpref.Nearest()
	}
	return readpref.Primary()
}

func docSliceToInterfaceSlice(docs []bson.D) []interface{} {
	out := make([]interface{}, 0, len(docs))

	for _, doc := range docs {
		out = append(out, doc)
	}

	return out
}

func docSliceFromRaw(mt *mtest.T, raw json.RawMessage) []bson.D {
	mt.Helper()
	jsonBytes, err := raw.MarshalJSON()
	assert.Nil(mt, err, "unable to marshal data: %v", err)

	array := bson.A{}
	err = bson.UnmarshalExtJSON(jsonBytes, true, &array)
	assert.Nil(mt, err, "unable to unmarshal data: %v", err)

	docs := make([]bson.D, 0)

	for _, val := range array {
		docs = append(docs, val.(bson.D))
	}

	return docs
}
