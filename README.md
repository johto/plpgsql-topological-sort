I needed a topological sort in PL/PgSQL, so here it is.  This is probably not
especially fast, but it's written in pure PL/PgSQL using only hstore.

Example usage with the graph on the Wikipedia page:

```SQL
SELECT topological_sort(
    ARRAY[5,7,3,11,8,2,9,10],
    hstore '11 => "{5,7}", 8 => "{7,3}", 2 => "{11}", 9 => "{8,11}", 10 => "{3,11}"'
);
```

result:

```
  topological_sort
---------------------
 {5,7,3,11,8,2,10,9}
(1 row)
```
