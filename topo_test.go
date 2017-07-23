package main

import (
	"database/sql"
	"github.com/lib/pq"
	"github.com/lib/pq/hstore"
	"testing"
)

type BenchType int
const (
	BENCH_JSON BenchType = iota
	BENCH_HSTORE
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

func bench(b *testing.B, vertices []int64, edges hstore.Hstore, benchType BenchType) {
	dbh, err := sql.Open("postgres", "sslmode=disable")
	if err != nil {
		b.Fatal(err)
	}
	err = dbh.Ping()
	if err != nil {
		b.Fatal(err)
	}
	eval, err := edges.Value()
	if err != nil {
		b.Fatal(err)
	}
	evalstr := eval.([]byte)
	var exec *sql.Stmt
	if benchType == BENCH_JSON {
		err = dbh.QueryRow(`
			select jsonb_object_agg(k, v)
			from each($1::hstore) e(k, v);
		`, evalstr).Scan(&evalstr)
		if err != nil {
			b.Fatal(err)
		}
		exec, err = dbh.Prepare(`SELECT topological_sort($1, $2::jsonb)`)
	} else if benchType == BENCH_HSTORE {
		exec, err = dbh.Prepare(`SELECT topological_sort($1, $2::hstore)`)
	}
	if err != nil {
		b.Fatal(err)
	}
	arr, err := pq.Int64Array(vertices).Value()
	if err != nil {
		b.Fatal(err)
	}
	arrstr := arr.(string)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = exec.Exec(arrstr, evalstr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSmallJSON(b *testing.B) {
	bench(b, verticesSmall, edgesSmall, BENCH_JSON)
}

func BenchmarkMediumJSON(b *testing.B) {
	bench(b, verticesMedium, edgesMedium, BENCH_JSON)
}

func BenchmarkLargeJSON(b *testing.B) {
	bench(b, verticesLarge, edgesLarge, BENCH_JSON)
}

func BenchmarkSmall(b *testing.B) {
	bench(b, verticesSmall, edgesSmall, BENCH_HSTORE)
}

func BenchmarkMedium(b *testing.B) {
	bench(b, verticesMedium, edgesMedium, BENCH_HSTORE)
}

func BenchmarkLarge(b *testing.B) {
	bench(b, verticesLarge, edgesLarge, BENCH_HSTORE)
}
