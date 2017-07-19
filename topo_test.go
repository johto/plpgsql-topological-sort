package main

import (
	"database/sql"
	_ "github.com/lib/pq"
	"testing"
)

func testSetup(t *testing.T) *sql.DB {
	dbh, err := sql.Open("postgres", "sslmode=disable")
	if err != nil {
		panic(err)
	}
	err = dbh.Ping()
	if err != nil {
		panic(err)
	}
	return dbh
}

func TestTopologicalSort(t *testing.T) {
	dbh := testSetup(t)
	// Should really probably have some more test cases here, and a nice way to
	// verify them without assuming a specific order. TODO
	_, err := dbh.Exec(`SELECT topological_sort(
		ARRAY[5,7,3,11,8,2,9,10],
		hstore '11 => "{5,7}", 8 => "{7,3}", 2 => "{11}", 9 => "{8,11}", 10 => "{3,11}"'
	)`)
	if err != nil {
		t.Fatal(err)
	}
}


func TestAllCycle(t *testing.T) {
	dbh := testSetup(t)

	_, err := dbh.Exec(`SELECT topological_sort(
		ARRAY[1,2],
		hstore '2 => "{1}", 1 => "{2}"'
	)`)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if err.Error() != "pq: no nodes with no incoming edges in input" {
		t.Fatalf(`expected "no nodes with no incoming edges in input", got %s`, err.Error())
	}
}

func TestCycle(t *testing.T) {
	dbh := testSetup(t)

	_, err := dbh.Exec(`SELECT topological_sort(
		ARRAY[1,2,3,4,5],
		hstore '2 => "{1,5}", 3 => "{2}", 4 => "{3}", 5 => "{4}"'
	)`)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if err.Error() != "pq: input graph contains cycles" {
		t.Fatalf(`expected "input graph contains cycles", got %s`, err.Error())
	}
}
