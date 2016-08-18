create or replace function test_win_til() returns INTEGER AS $$
DECLARE
	tiles_stack char(8)[] ;
	node_ids BIGINT[];
	way_ids BIGINT[];
	rel_ids BIGINT[];
	
BEGIN
	
	tiles_stack = window2tiles(13.993803, 35.698568, 14.607275, 36.183668);
	return 0;

END;
$$ LANGUAGE PLPGSQL;

create or replace function test_til_nod() returns INTEGER AS $$
DECLARE
	tiles_stack char(8)[] ;
	node_ids BIGINT[];
	way_ids BIGINT[];
	rel_ids BIGINT[];
	wx1 real := 13.993803;
	wy1 real := 35.698568;
	wx2 real := 14.607275;
	wy2 real := 36.183668;

BEGIN
	tiles_stack = window2tiles(wx1, wy1, wx2, wy2);
	
	node_ids = tiles2node_ids(tiles_stack, wx1, wy1, wx2, wy2);
	
	return 0;

END;
$$ LANGUAGE PLPGSQL;

create or replace function test_way_ids() returns INTEGER AS $$
DECLARE
	tiles_stack char(8)[] ;
	node_ids BIGINT[];
	way_ids BIGINT[];
	rel_ids BIGINT[];

	wx1 real := 13.993803;
	wy1 real := 35.698568;
	wx2 real := 14.607275;
	wy2 real := 36.183668;

BEGIN
	tiles_stack = window2tiles(wx1, wy1, wx2, wy2);
	
	node_ids = tiles2node_ids(tiles_stack, wx1, wy1, wx2, wy2);
	
	way_ids = node_ids2way_ids(node_ids);

	return 0;

END;
$$ LANGUAGE PLPGSQL;

create or replace function test_way_trees() returns INTEGER AS $$
DECLARE
	tiles_stack char(8)[] ;
	node_ids BIGINT[];
	way_ids BIGINT[];

	rel_ids BIGINT[];

	way_tree_node_ids BIGINT[];

	wx1 real := 13.993803;
	wy1 real := 35.698568;
	wx2 real := 14.607275;
	wy2 real := 36.183668;

BEGIN

	tiles_stack = window2tiles(wx1, wy1, wx2, wy2);
	
	node_ids = tiles2node_ids(tiles_stack, wx1, wy1, wx2, wy2);
	
	way_ids = node_ids2way_ids(node_ids);

	way_tree_node_ids = way_nds2way_trees(way_ids, node_ids);
	
	return 0;

END;
$$ LANGUAGE PLPGSQL;
