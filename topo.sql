CREATE OR REPLACE FUNCTION topological_sort(_nodes int[], _edges hstore)
RETURNS int[]
AS $$
DECLARE
_L int[];
_S int[];
_n int;
_all_ms text[];
_m text;
_n_m_edges int[];
BEGIN
_L := '{}';
_S := ARRAY(
	SELECT u.node
	FROM unnest(_nodes) u(node)
	WHERE (_edges->(u.node::text)) IS NULL
);
IF array_length(_S, 1) IS NULL THEN
	RAISE EXCEPTION 'no nodes with no incoming edges in input';
END IF;

WHILE array_length(_S, 1) IS NOT NULL LOOP
	_n := _S[1];
	_S := _S[2:];

	_L := array_append(_L, _n);
	_all_ms := ARRAY(
		SELECT each.key
		FROM each(_edges)
		WHERE (each.value)::int[] @> ARRAY[_n]
	);
	FOREACH _m IN ARRAY _all_ms LOOP
		_n_m_edges := (_edges->_m)::int[];
		IF _n_m_edges = ARRAY[_n] THEN
			_S := array_append(_s, _m::int);
			_edges := _edges - _m;
		ELSE
			_edges := _edges || hstore(_m, array_remove(_n_m_edges, _n)::text);
		END IF;
	END LOOP;
END LOOP;
IF array_length(akeys(_edges), 1) IS NOT NULL THEN
	RAISE EXCEPTION 'input graph contains cycles';
END IF;
RETURN _L;
END
$$ LANGUAGE plpgsql;
