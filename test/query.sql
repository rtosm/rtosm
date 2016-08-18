create or replace function windowing_query(wx1 REAL, wy1 REAL, wx2 REAL, wy2 REAL) returns integer AS $$
DECLARE
	tiles_stack char(8)[] ;
	node_ids BIGINT[];
	way_ids BIGINT[];
	rel_ids BIGINT[];

	way_tree_node_ids BIGINT[];
	rel_tree_node_ids BIGINT[];
BEGIN
	tiles_stack = window2tiles(wx1, wy1, wx2, wy2);

	node_ids = tiles2node_ids(tiles_stack, wx1, wy1, wx2, wy2);

	way_ids = node_ids2way_ids(node_ids);

	rel_ids = node_ids2rel_ids(node_ids);

	way_tree_node_ids = way_nds2way_trees(way_ids, node_ids);

	rel_tree_node_ids = rel_nds2rel_trees(rel_ids, node_ids);

	-- combile all the results, return it to the request 

	-- do we need to encode the output in OSM file ?

	return 0;

END;
$$ LANGUAGE PLPGSQL;

create or replace function window2tiles(wx1 real, wy1 real, wx2 real, wy2 real) returns CHAR(8)[] AS $$
DECLARE
	xspan real := wx2 - wx1;
	yspan real := wy2 - wy1;

	xerror REAL := xspan / 2048.0 * 3.0 ;
	yerror REAL := yspan / 2048.0 * 3.0 ;

	major_error REAL ;

	tile1s char(8)[];
	tile2s char(8)[];
	tile3s char(8)[];
	tile4s char(8)[];

	distinct_tiles CHAR(8)[];
BEGIN
	if xerror > yerror then
		major_error = xerror;
	else
		major_error = yerror;
	end if;

	-- bottom up to get all related tiles
	while major_error < 0.55 loop
		tile1s = tile1s || gcode_c(wx1, wy1, major_error)::CHAR(8); 
		tile2s = tile2s || gcode_c(wx1, wy2, major_error)::CHAR(8); 
		tile3s = tile3s || gcode_c(wx2, wy2, major_error)::CHAR(8); 
		tile4s = tile4s || gcode_c(wx2, wy1, major_error)::CHAR(8);
		major_error = major_error * 2.0;
	end loop;

	-- eliminate the duplication in tiles

	select array_agg(distinct tl order by tl) into distinct_tiles from (
	select unnest(tile1s) as tl  union
	select unnest(tile2s) union
	select unnest(tile3s) union
	select unnest(tile4s) ) tls;

	raise info '%', distinct_tiles;

	return distinct_tiles;

END;
$$ LANGUAGE PLPGSQL;

create or replace function tiles2node_ids(tiles char(8)[], wx1 REAL, wy1 REAL, wx2 REAL, wy2 REAL) returns BIGINT[] AS $$ 
DECLARE
	
	node_ids BIGINT[];

BEGIN

	-- the spatial filter on the table join is most time consuming
	-- alternative query for the nodes id
	select array_agg(cn.id) into node_ids from 
					(select node_id from node_vis_errors where tile = ANY(tiles)) nve
					 inner join current_nodes cn on cn.id = nve.node_id and 
						cn.longitude >= wx1 * 10000000 and cn.longitude <= wx2 * 10000000 
						and cn.latitude >= wy1 * 10000000 and cn.latitude <= wy2 * 10000000;

	-- all node_ids whose node is assocated with specific tile and fall in the target window
	--select array_agg(nve.node_id) into node_ids from node_vis_errors nve inner join
			--current_nodes cn on nve.node_id = cn.id and nve.tile = ANY(tiles) and 
			--cn.longitude >= wx1 * 10000000 and cn.longitude <= wx2 * 10000000 and cn.latitude >= wy1 * 10000000 and cn.latitude <= wy2 * 10000000;

	raise info 'the number of the nodes in windows is %', array_length(node_ids, 1); 
	--raise info '%', node_ids;

	return node_ids;
END;
$$ LANGUAGE PLPGSQL;

create or replace function node_ids2way_ids(node_ids BIGINT[]) returns BIGINT[] AS $$
DECLARE
	way_ids BIGINT[];
BEGIN
	-- get all the way_id whose node in the node_ids
	select array_agg(distinct way_id order by way_id) into way_ids from way_trees where node_id = ANY(node_ids) ;

	raise info 'the number of the way_ids is %', array_length(way_ids, 1); 
	--raise info '%', way_ids;
	return way_ids;

END;
$$ LANGUAGE PLPGSQL;

create or replace function node_ids2rel_ids(node_ids BIGINT[]) returns BIGINT[] AS $$
DECLARE
	rel_ids BIGINT[];

BEGIN
	-- get all the relation_id whose node in the node_ids
	select array_agg(distinct relation_id order by relation_id) into rel_ids from relation_trees where node_id = ANY(node_ids);
	
	raise info 'the number of the rel_ids is %', array_length(rel_ids, 1); 
	--raise info '%', rel_ids;
	return rel_ids;

END;
$$ LANGUAGE PLPGSQL;

-- traverse all the ways and get the ancestors of the nodes in the target window.
create or replace function way_nds2way_trees(way_ids BIGINT[], node_ids BIGINT[]) returns BIGINT[] AS $$
DECLARE

	w_id BIGINT;
	w_tns way_trees[];

	w_tn way_trees;
	w_tn_ancestor_paths CHAR(8)[];
	ancestor_paths CHAR(8)[];
	w_tpaths CHAR(8)[];

	tnodes_ids BIGINT[];
	trees_nodes_ids BIGINT[];
BEGIN

	if way_ids is null then 
		return null;
	end if;

	foreach w_id in ARRAY way_ids loop
		-- select the way's qualified node in target window into an array
		select array_agg(wt.* order by wt.path) into w_tns from way_trees wt where way_id = w_id and node_id = ANY(node_ids);
		ancestor_paths = null;
		-- get all the parents of the qualified and windowed nodes
		foreach w_tn in ARRAY w_tns loop
			w_tn_ancestor_paths = get_ancestors(w_tn.path);
			ancestor_paths = ancestor_paths || w_tn_ancestor_paths;
		end loop;

		-- remove the duplication
		select array_agg(distinct pth order by pth) into w_tpaths from (select unnest(ancestor_paths) as pth) ps;

		-- from paths to node_ids
		select array_agg(distinct wt.node_id) into tnodes_ids from way_trees wt where way_id = w_id and path = ANY(w_tpaths);

		-- concatenate the node_ids
		trees_nodes_ids = trees_nodes_ids || tnodes_ids;
	end loop;
	return trees_nodes_ids;

END;
$$ LANGUAGE PLPGSQL;

create or replace function rel_nds2rel_trees(rel_ids BIGINT[], node_ids BIGINT[]) returns BIGINT[] AS $$
DECLARE
	r_id BIGINT;
	r_tns relation_trees[];
	r_tn relation_trees;

	r_tn_ancestor_paths CHAR(8)[];
	ancestor_paths CHAR(8)[];

	r_tpaths CHAR(8)[];

	r_nodes_ids BIGINT[];
	trees_nodes_ids BIGINT[];

BEGIN
	-- iterate over the rel_ids to retrieve relation's in-window nodes 
	foreach r_id in ARRAY rel_ids loop
		select array_agg(rt.* order by rt.path) into r_tns from relation_trees rt where relation_id = r_id and node_id = ANY(node_ids);
		ancestor_paths = null;
		foreach r_tn in ARRAY r_tns loop
			r_tn_ancestor_paths = get_ancestors(r_tn.path);
			ancestor_paths = ancestor_paths || r_tn_ancestor_paths;
		end loop;

		-- remove duplications
		select array_agg(distinct pth order by pth) into r_tpaths from (select unnest(ancestor_paths) as pth) ps;

		-- from paths to node_ids
		select array_agg(distinct rt.node_id) into  r_nodes_ids from relation_trees rt where relation_id = r_id and path = ANY(r_tpaths);

		-- concatenate the ids
		trees_nodes_ids = trees_nodes_ids || r_nodes_ids;
	end loop;

	return trees_nodes_ids;

END;
$$ LANGUAGE PLPGSQL;
 
create or replace function way_nds2way_tree(each_way_id BIGINT, node_ids BIGINT[]) returns BIGINT[] as $$ 
DECLARE

BEGIN

END;
$$ LANGUAGE PLPGSQL;

create or replace function rel_nds2rel_tree(each_rel_id BIGINT, node_ids BIGINT[]) returns BIGINT[] AS $$
DECLARE

BEGIN

END;
$$ LANGUAGE PLPGSQL;
