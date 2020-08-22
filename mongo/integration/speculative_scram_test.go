package integration

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/internal/testutil/assert"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

func TestSpeculativeSCRAMAuthentication(t *testing.T) {
	// Integration tests for speculative SCRAM authentication. Speculative MONGODB-X509 auth can't be integration
	// tested because these tests use the mongotest proxy dialer functionality, which cannot run in TLS environments.

	mtOpts := mtest.NewOptions().
		Auth(true).
		CreateClient(false).
		MinServerVersion("4.0") // SCRAM-SHA-256 is only supported on 4.0+
	mt := mtest.New(t, mtOpts)
	defer mt.Close()

	speculativeAuthEnabled := mtest.CompareServerVersions(mt.ServerVersion(), "4.4") >= 0
	authMtOpts := mtest.NewOptions().
		CreateCollection(false).
		ClientType(mtest.Proxy)

	testCases := []struct {
		name              string
		mechanism         string
		expectedMechanism string
	}{
		{"scram-sha-1", "SCRAM-SHA-1", "SCRAM-SHA-1"},
		{"scram-sha-256", "SCRAM-SHA-256", "SCRAM-SHA-256"},
		{"default", "", "SCRAM-SHA-256"},
	}
	for _, tc := range testCases {
		mt.RunOpts(tc.name, authMtOpts, func(mt *mtest.T) {
			// Reset the test client to use the specified auth mechanism.
			clientOptions := mt.ClientOptions()
			clientOptions.Auth.AuthMechanism = tc.mechanism
			mt.ResetClient(clientOptions)

			err := mt.Client.Ping(mtest.Background, readpref.PrimaryPreferred())
			assert.Nil(mt, err, "Ping error: %v", err)

			// Verify that the correct number of commands were sent in the connection handshake.
			// For pre-4.4 servers, we send isMaster, saslStart, saslContinue, and saslContinue.
			// For 4.4+ servers, we send speculative isMaster, saslContinue.
			commands := filterProxiedConnectionHandshake(mt)
			expectedNumCommands := 4
			if speculativeAuthEnabled {
				expectedNumCommands = 2
			}
			assert.Equal(mt, len(commands), expectedNumCommands,
				"expected %d commands sent in the auth handshake, got %d", expectedNumCommands, len(commands))

			// Verify the structure of the speculativeAuthenticate document in the first isMaster.
			actualSpeculativeDoc, ok := commands[0].Lookup("speculativeAuthenticate").DocumentOK()
			assert.True(mt, ok, "expected command %s to contain 'speculativeAuthenticate' sub-document", commands[0])

			expectedSpeculativeDoc := bsoncore.NewDocumentBuilder().
				AppendInt32("saslStart", 1).
				AppendString("mechanism", tc.expectedMechanism).
				// The exact payload is unknown, so we use 42 to only assert that it exists in the actual document.
				AppendInt32("payload", 42).
				AppendString("db", "admin").
				AppendDocument("options", bsoncore.NewDocumentBuilder().
					AppendBoolean("skipEmptyExchange", true).
					Build(),
				).
				Build()
			err = compareDocs(mt, bson.Raw(expectedSpeculativeDoc), bson.Raw(actualSpeculativeDoc))
			assert.Nil(mt, err, "compareDocs error: %v", err)
		})
	}
}

// This function extracts the commands sent as part of an application connection handshake from the full list of proxied
// messages.
func filterProxiedConnectionHandshake(mt *mtest.T) []bsoncore.Document {
	var filtered []bsoncore.Document
	for _, msg := range mt.GetProxiedMessages() {
		if msg.CommandName == "isMaster" {
			if _, err := msg.Sent.Command.LookupErr("speculativeAuthenticate"); err != nil {
				continue
			}
		}
		if msg.CommandName != "saslStart" && msg.CommandName != "saslContinue" {
			continue
		}

		filtered = append(filtered, msg.Sent.Command)
	}

	return filtered
}
