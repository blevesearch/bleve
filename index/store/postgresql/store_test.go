package postgresql

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/index/store/test"

	// needed because we initialize the database connection instance in this file
	_ "github.com/lib/pq"
)

const (
	testTable = "bleve_test"
)

var (
	testDataSourceName string
)

func setup() {
	dbHost, ok := os.LookupEnv("POSTGRES_HOST")
	if !ok {
		log.Fatal("POSTGRES_HOST environment variable not set")
	}

	dbPort, ok := os.LookupEnv("POSTGRES_PORT")
	if !ok {
		log.Fatal("POSTGRES_PORT environment variable not set")
	}

	dbUser, ok := os.LookupEnv("POSTGRES_USER")
	if !ok {
		log.Fatal("POSTGRES_USER environment variable not set")
	}

	dbPassword, ok := os.LookupEnv("POSTGRES_PASSWORD")
	if !ok {
		log.Fatal("POSTGRES_PASSWORD environment variable not set")
	}

	db, ok := os.LookupEnv("POSTGRES_DATABASE")
	if !ok {
		log.Fatal("POSTGRES_DATABASE environment variable not set")
	}

	testDataSourceName = fmt.Sprintf(
		"postgresql://%s:%s@%s:%s/%s?sslmode=disable",
		dbUser,
		dbPassword,
		dbHost,
		dbPort,
		db,
	)
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	os.Exit(code)
}

func truncateTable(t *testing.T) {
	db, err := sql.Open("postgres", testDataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("TRUNCATE %s;", testTable))
	if err != nil {
		t.Fatal(err)
	}
}

func open(t *testing.T, mo store.MergeOperator) store.KVStore {
	rv, err := New(mo, map[string]interface{}{
		"datasourceName": testDataSourceName,
		"table":          testTable,
		"keyCol":         "my_key",
		"valCol":         "my_val",
	})
	if err != nil {
		t.Fatal(err)
	}

	return rv
}

func cleanup(t *testing.T, s store.KVStore) {
	err := s.Close()
	if err != nil {
		t.Fatal(err)
	}

	truncateTable(t)
}

func TestPostgreSQLKVCrud(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestKVCrud(t, s)
}

func TestPostgreSQLeaderIsolation(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestReaderIsolation(t, s)
}

func TestPostgreSQLReaderOwnsGetBytes(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestReaderOwnsGetBytes(t, s)
}

func TestPostgreSQLWriterOwnsBytes(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestWriterOwnsBytes(t, s)
}

func TestPostgreSQLPrefixIterator(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestPrefixIterator(t, s)
}

func TestPostgreSQLPrefixIteratorSeek(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestPrefixIteratorSeek(t, s)
}

func TestPostgreSQLRangeIterator(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestRangeIterator(t, s)
}

func TestPostgreSQLRangeIteratorSeek(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestRangeIteratorSeek(t, s)
}

func TestPostgreSQLMerge(t *testing.T) {
	s := open(t, &test.TestMergeCounter{})
	defer cleanup(t, s)
	test.CommonTestMerge(t, s)
}
