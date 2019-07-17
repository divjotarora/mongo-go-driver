package integration

import (
	"encoding/json"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/internal/testutil/assert"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io/ioutil"
	"path"
	"testing"
)

type testFile struct {
	RunOn          []mtest.RunOnBlock `json:"runOn"`
	DatabaseName   string              `json:"database_name"`
	CollectionName string              `json:"collection_name"`
	Data           json.RawMessage     `json:"data"`
	Tests          []testCase         `json:"tests"`
}

type testCase struct {
	Description         string                 `json:"description"`
	SkipReason          string                 `json:"skipReason"`
	FailPoint           *mtest.FailPoint       `json:"failPoint"`
	ClientOptions       map[string]interface{} `json:"clientOptions"`
	SessionOptions      map[string]interface{} `json:"sessionOptions"`
	Operations          []*oper                `json:"operations"`
	Expectations        []*expectation         `json:"expectations"`
	UseMultipleMongoses bool                   `json:"useMultipleMongoses"`

	Outcome *struct {
		Collection struct {
			Data json.RawMessage `json:"data"`
		} `json:"collection"`
	} `json:"outcome"`
}

type expectation struct {
	CommandStartedEvent struct {
		CommandName  string          `json:"command_name"`
		DatabaseName string          `json:"database_name"`
		Command      json.RawMessage `json:"command"`
	} `json:"command_started_event"`
}

type oper struct {
	Name              string                 `json:"name"`
	Object            string                 `json:"object"`
	CollectionOptions map[string]interface{} `json:"collectionOptions"`
	Result            json.RawMessage        `json:"result"`
	Arguments         json.RawMessage        `json:"arguments"`
	ArgMap            map[string]interface{}
	Error             bool `json:"error"`
}

const dataPath string = "../../data/"

var directories = []string{
	"transactions",
	"crud/v2",
}

func TestNewSpecs(t *testing.T) {
	for _, specName := range directories {
		t.Run(specName, func(t *testing.T) {
			for _, fileName := range findJSONFilesInDir(t, path.Join(dataPath, specName)) {
				t.Run(fileName, func(t *testing.T) {
					runSpecTestFile(t, path.Join(dataPath, specName, fileName))
				})
			}
		})
	}
}

func runSpecTestFile(t *testing.T, filePath string) {
	// setup: read test file
	content, err := ioutil.ReadFile(filePath)
	assert.Nil(t, err, "unable to read spec test file at: %v", filePath)

	var testFile testFile
	err = json.Unmarshal(content, &testFile)
	assert.Nil(t, err, "unable to unmarshall spec test file at; %v", filePath)

	// create mtest wrapper and skip if needed
	mt := mtest.New(t, mtest.NewOptions().Constraints(testFile.RunOn...))

	if mt.TopologyKind() == mtest.ReplicaSet {
		err := mt.Client.Database("admin").RunCommand(mtest.Background, bson.D{
			{"setParameter", 1},
			{"transactionLifetimeLimitSeconds", 3},
		}).Err()
		assert.Nil(mt, err, "unable to set transactionLifetimeLimitSeconds: %v", err)
	}

	// TODO: to run test in parallel, we need to fix sanitizeCollectioName
	collName := sanitizeCollectionName(testFile.DatabaseName, testFile.CollectionName)
	for _, test := range testFile.Tests {
		runSpecTestCase(mt, test, testFile, collName)
	}
}

func runSpecTestCase(mt *mtest.T, test testCase, testFile testFile, collName string) {
	clientOpts := createClientOptions(test.ClientOptions)
	opts := mtest.NewOptions().DatabaseName(testFile.DatabaseName).CollectionName(collName).ClientOptions(clientOpts)
	if mt.TopologyKind() == mtest.Sharded && !test.UseMultipleMongoses {
		// pin to a single mongos
		opts = opts.ClientType(mtest.Pinned)
	}

	mt.RunOpts(test.Description, opts, func(mt *mtest.T) {
		// TODO disable fail points
		if len(test.SkipReason) > 0 {
			mt.Skip(test.SkipReason)
		}

		killSessions(mt)
	})
}


func setUpTest(mt *mtest.T, test testCase, testFile testFile) {
	err := mt.Coll.Database().RunCommand(mtest.Background, bson.D{
		{"create", mt.Coll.Name()},
	}).Err()
	assert.Nil(mt, err, "unable to create collection: %v", err)
	docsToInsert := docSliceToInterfaceSlice(docSliceFromRaw(mt, testFile.Data))
	if len(docsToInsert) > 0{
		insertColl, err := mt.Coll.Clone(options.Collection().SetWriteConcern(mtest.MajorityWc))
		assert.Nil(mt, err, "unable to clone collection: %v", err)
		_, err = insertColl.InsertMany(mtest.Background, docsToInsert)
		assert.Nil(mt, err, "unable to insert documents into collection: %v", err)
	}

	if test.FailPoint != nil {
		mt.SetFailPoint(*test.FailPoint)
	}
}
