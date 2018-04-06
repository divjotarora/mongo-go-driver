package integration

import (
	"context"
	"testing"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/core/command"
	"github.com/mongodb/mongo-go-driver/core/description"
	"github.com/mongodb/mongo-go-driver/core/options"
	"github.com/mongodb/mongo-go-driver/internal/testutil"
)

func TestCommandListIndexes(t *testing.T) {
	noerr := func(t *testing.T, err error) {
		// t.Helper()
		if err != nil {
			t.Errorf("Unepexted error: %v", err)
			t.FailNow()
		}
	}
	t.Run("InvalidDatabaseName", func(t *testing.T) {
		server, err := testutil.Topology(t).SelectServer(context.Background(), description.WriteSelector())
		noerr(t, err)
		conn, err := server.Connection(context.Background())
		noerr(t, err)
		ns := command.Namespace{DB: "ex", Collection: "space"}
		cursor, err := (&command.ListIndexes{NS: ns}).RoundTrip(context.Background(), server.SelectedDescription(), server, conn)
		noerr(t, err)

		indexes := []string{}
		var next *bson.Document

		for cursor.Next(context.Background()) {
			err = cursor.Decode(next)
			noerr(t, err)

			elem, err := next.Lookup("name")
			noerr(t, err)
			if elem.Value().Type() != bson.TypeString {
				t.Errorf("Incorrect type for 'name'. got %v; want %v", elem.Value().Type(), bson.TypeString)
				t.FailNow()
			}
			indexes = append(indexes, elem.Value().StringValue())
		}

		if len(indexes) != 0 {
			t.Errorf("Expected no indexes from invalid database. got %d; want %d", len(indexes), 0)
		}
	})
	t.Run("InvalidCollectionName", func(t *testing.T) {
		server, err := testutil.Topology(t).SelectServer(context.Background(), description.WriteSelector())
		noerr(t, err)
		conn, err := server.Connection(context.Background())
		noerr(t, err)
		ns := command.Namespace{DB: "ex", Collection: testutil.ColName(t)}
		cursor, err := (&command.ListIndexes{NS: ns}).RoundTrip(context.Background(), server.SelectedDescription(), server, conn)
		noerr(t, err)

		indexes := []string{}
		var next *bson.Document

		for cursor.Next(context.Background()) {
			err = cursor.Decode(next)
			noerr(t, err)

			elem, err := next.Lookup("name")
			noerr(t, err)
			if elem.Value().Type() != bson.TypeString {
				t.Errorf("Incorrect type for 'name'. got %v; want %v", elem.Value().Type(), bson.TypeString)
				t.FailNow()
			}
			indexes = append(indexes, elem.Value().StringValue())
		}

		if len(indexes) != 0 {
			t.Errorf("Expected no indexes from invalid database. got %d; want %d", len(indexes), 0)
		}
	})
	t.Run("SingleBatch", func(t *testing.T) {
		server, err := testutil.Topology(t).SelectServer(context.Background(), description.WriteSelector())
		noerr(t, err)
		conn, err := server.Connection(context.Background())
		noerr(t, err)
		testutil.AutoDropCollection(t)
		testutil.AutoCreateIndexes(t, []string{"a"})
		testutil.AutoCreateIndexes(t, []string{"b"})
		testutil.AutoCreateIndexes(t, []string{"c"})
		testutil.AutoCreateIndexes(t, []string{"d", "e"})

		ns := command.NewNamespace(dbName, testutil.ColName(t))
		cursor, err := (&command.ListIndexes{NS: ns}).RoundTrip(context.Background(), server.SelectedDescription(), server, conn)
		noerr(t, err)

		indexes := []string{}
		next := bson.NewDocument()

		for cursor.Next(context.Background()) {
			err = cursor.Decode(next)
			noerr(t, err)

			elem, err := next.Lookup("name")
			noerr(t, err)
			if elem.Value().Type() != bson.TypeString {
				t.Errorf("Incorrect type for 'name'. got %v; want %v", elem.Value().Type(), bson.TypeString)
				t.FailNow()
			}
			indexes = append(indexes, elem.Value().StringValue())
		}

		if len(indexes) != 5 {
			t.Errorf("Incorrect number of indexes. got %d; want %d", len(indexes), 5)
		}
		for i, want := range []string{"_id_", "a", "b", "c", "d_e"} {
			got := indexes[i]
			if got != want {
				t.Errorf("Mismatched index %d. got %s; want %s", i, got, want)
			}
		}
	})
	t.Run("MultipleBatch", func(t *testing.T) {
		server, err := testutil.Topology(t).SelectServer(context.Background(), description.WriteSelector())
		noerr(t, err)
		conn, err := server.Connection(context.Background())
		noerr(t, err)
		testutil.AutoDropCollection(t)
		testutil.AutoCreateIndexes(t, []string{"a"})
		testutil.AutoCreateIndexes(t, []string{"b"})
		testutil.AutoCreateIndexes(t, []string{"c"})

		ns := command.NewNamespace(dbName, testutil.ColName(t))
		cursor, err := (&command.ListIndexes{NS: ns, Opts: []options.ListIndexesOptioner{options.OptBatchSize(1)}}).RoundTrip(context.Background(), server.SelectedDescription(), server, conn)
		noerr(t, err)

		indexes := []string{}
		next := bson.NewDocument()

		for cursor.Next(context.Background()) {
			err = cursor.Decode(next)
			noerr(t, err)

			elem, err := next.Lookup("name")
			noerr(t, err)
			if elem.Value().Type() != bson.TypeString {
				t.Errorf("Incorrect type for 'name'. got %v; want %v", elem.Value().Type(), bson.TypeString)
				t.FailNow()
			}
			indexes = append(indexes, elem.Value().StringValue())
		}

		if len(indexes) != 4 {
			t.Errorf("Incorrect number of indexes. got %d; want %d", len(indexes), 5)
		}
		for i, want := range []string{"_id_", "a", "b", "c"} {
			got := indexes[i]
			if got != want {
				t.Errorf("Mismatched index %d. got %s; want %s", i, got, want)
			}
		}
	})
}
