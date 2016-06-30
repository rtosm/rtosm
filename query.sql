
-- this is the query to respond a windowing query.
create or replace function sQuery(x1 real, y1 real, x2 real, y2 real) RETURNS text AS $$
DECLARE
	sqcursor CURSOR(overlapped_tiles CHAR(8)[]) for select rtns.relation_id, rtns.node_id, rtns.path, cns.longitude, cns.latitude from current_nodes cns inner join (select relation_id, node_id, path from relation_trees where tile in (select unnest(overlapped_tiles)) order by relation_id, path) rtns on rtns.node_id = cns.id;

	retrieved_tiles CHAR(8)[];
	count INTEGER := 0;

	rel_id BIGINT;
	tn_id BIGINT;
	tn_path CHAR(8);
	lon	double precision;
	lat double precision;

	prev_rel_id BIGINT = -1;
	wkt_geometries text;
BEGIN
	wkt_geometries = 'MULTIPOLYGON((';

	retrieved_tiles = spatial_condition(x1, y1, x2, y2);
	-- using a refcursor here ?
	open sqcursor(retrieved_tiles);
	loop 
		fetch next from sqcursor into rel_id, tn_id, tn_path, lon, lat ;
		if FOUND then
			if prev_rel_id <> rel_id then
				wkt_geometries = wkt_geometries || '(' || lon / 10000000.0 || ' ' || lat / 10000000.0 ;
				prev_rel_id = rel_id ; 
			else 
				wkt_geometries = wkt_geometries || ',' || lon / 10000000.0 || ' ' || lat / 10000000.0 ;
			end if;
			count = count + 1;
		else
			raise info 'Cursor reaches the end.';
			exit;
		end if;
	end loop;

	wkt_geometries = wkt_geometries || ')))';
	
	-- assembly the node into ways and relations

	-- craft the relation and ways into the wkt text
	return wkt_geometries;
END;
$$ LANGUAGE PLPGSQL;

-- this is the function to 
create or replace function spatial_condition(x1 real, y1 real, x2 real, y2 real) returns CHAR(8)[] AS $$
DECLARE
	x_span REAL;
	y_span REAL;

	x_tolerance REAL;
	y_tolerance REAL;
	tolerance REAL;

	lb_point_tiles CHAR(8)[];
	lt_point_tiles CHAR(8)[];
	tr_point_tiles CHAR(8)[];
	br_point_tiles CHAR(8)[];

	union_tiles CHAR(8)[];
	distinct_tiles CHAR(8)[];
BEGIN
	-- from viewbox to 3-pixel tolerance
	x_span = x2 - x1;
	y_span = y2 - y1;

	-- use the span to calculate the depth in vertical direction
	-- use the box_geohash_encode function to get the geohash string
	x_tolerance = x_span / 2048 * 3;
	y_tolerance = y_span / 2048 * 3;

	if x_tolerance > y_tolerance then
		tolerance = x_tolerance;
	else
		tolerance = y_tolerance;
	end if;

	if tolerance < 0.000002 then tolerance = 0.0000015; end if;
	while tolerance < 0.54 loop
		lb_point_tiles = lb_point_tiles || gcode_c(x1, y1, tolerance);
		lt_point_tiles = lt_point_tiles || gcode_c(x1, y2, tolerance);
		tr_point_tiles = tr_point_tiles || gcode_c(x2, y2, tolerance);
		br_point_tiles = br_point_tiles || gcode_c(x2, y1, tolerance);
		tolerance = tolerance * 2;
	end loop;

	lb_point_tiles = lb_point_tiles || gcode_c(x1, y1, tolerance);
	lt_point_tiles = lt_point_tiles || gcode_c(x1, y2, tolerance);
	tr_point_tiles = tr_point_tiles || gcode_c(x2, y2, tolerance);
	br_point_tiles = br_point_tiles || gcode_c(x2, y1, tolerance);

	union_tiles = lb_point_tiles || lt_point_tiles || tr_point_tiles || br_point_tiles;

	distinct_tiles = array(select distinct unnest(union_tiles));

	return distinct_tiles;
end;
$$ LANGUAGE PLPGSQL;
