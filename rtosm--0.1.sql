-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION rtosm" to load this file. \quit

----------create and initialize all the necessary tables ----------------------------------------------------------------

-- there are three main tables for an DOOPAS(Data Operation-Orentied Picking and Assessing Structure) instance:
	-- the node error table, way tree table and relation tree table
create table if not exists node_vis_errors(node_id BIGINT not null,
														error real not null,
														tile CHAR(8) collate "C" not null, 
														primary key (node_id));

create table if not exists way_trees(way_id BIGINT not null, 
																			node_id BIGINT not null, 
																			error REAL not null, 
																			path CHAR(8) collate "C" not null, 
																			subsize INTEGER not null, 
																			--tile CHAR(8) collate "C" not null, 
																			primary key (way_id, node_id, path));

create table if not exists relation_trees (relation_id BIGINT not null, 
																					 node_id BIGINT not null,
																					 opening_member_id BIGINT,
																					 opening_member_type nwr_enum,
																					 closing_member_id BIGINT,
																					 closing_member_type nwr_enum,
																					 error REAL not null,
																					 path CHAR(8) COLLATE "C" not null, 
																					 subsize INTEGER not null, 
																					 --tile CHAR(8) COLLATE "C" not null,
																					 primary key (relation_id, node_id, path));


----------create and initialize all the necessary tables ----------------------------------------------------------------

----------node.sql----------------------------------------------------------------

-- create the node error table and fill it with initialization values.
create or replace function build_node_vis_errors() returns integer AS $$

DECLARE

BEGIN
	insert into node_vis_errors(node_id, error, tile) select id, -1, '0' from current_nodes;
	return 0;
END;

$$ LANGUAGE PLPGSQL;

----------node.sql----------------------------------------------------------------


----------way.sql----------------------------------------------------------------

--LOG
--change the relation_trees to contain the opening_member_id and closing_member_id and their types
	-- 20160706 remove the column 'tile' from way_trees because of the 'tile' column only presented
	-- in node_errors;

create or replace function build_way_trees() returns real as $$ 
DECLARE
waycur SCROLL CURSOR for select distinct way_id AS C from way_nodes;
cur_way_id BIGINT;

retval real;

BEGIN
	-- traverse the way_nodes table to calculate each node's error WRT ways.
	open waycur; 
	loop 
		fetch next from waycur into cur_way_id; 
		if FOUND THEN 
			retval = simpl_c(cur_way_id, 0.2);
		else
			exit;
		end if;
	end loop;
	close waycur;
	return 0;
END;
$$ LANGUAGE PLPGSQL;

----------way.sql----------------------------------------------------------------

----------relation.sql----------------------------------------------------------------

-- this is a try to spped up the relation_tree build for large relations
create or replace function build_relation_trees() RETURNS INTEGER AS $$
DECLARE
	relids BIGINT[];
	idnum INTEGER := 0;
	
	i INTEGER := 1;
	way_ids BIGINT[];
	way_num INTEGER := 0;

	nodes current_nodes[];
	nd_num INTEGER := 0;

	ts TIMESTAMP;
	te TIMESTAMP;

	root_err REAL := -1;
	ret_edpt INTEGER := 0;

	rndlmt INTEGER[];
	rndxs INTEGER[];
	rndys INTEGER[];
BEGIN
	-- create the table of relation_trees
	ts = timeofday()::TIMESTAMP;

	-- currently we build the realtion tree with 'boundary' and 'multipolygons'
	select array_agg(rts.relation_id) into relids from relation_tags rts where rts.k = 'type' and (rts.v = 'boundary' or rts.v = 'multipolygon');

	idnum = array_length(relids, 1);

	-- loop through relations to build tree
	while i <= idnum loop
		raise info 'the relation_tree : % begin to build', relids[i];

		-- read out id of ways and nodes of the ways' endpoints
		select w_ids, nds into way_ids, nodes from rws(relids[i]);

		way_num = array_length(way_ids, 1);

		--raise info '%', way_ids;
		if not way_ids is null then
			
			nd_num = array_length(nodes, 1);

			select ndlmt, ndxs, ndys into rndlmt, rndxs, rndys from ron(way_ids);
			--raise info 'the length of ndxs is %', array_length(rndxs, 1);

			--raise info '%', nds; 
			root_err = s(relids[i], way_ids, nodes, rndlmt, rndxs, rndys, 1, nd_num, 'T', 0.2);

			-- insert the endpoints of the relation
			ret_edpt = insert_relends(relids[i], root_err, way_ids[1], way_ids[way_num], way_num);
		else 
			raise info '% relation is null because of lack member way.', relids[i];

		end if; 
		i = i + 1;
	end loop;
	-- then with the boundary 

	te = timeofday()::TIMESTAMP;
	raise info 'the function cost: %', te - ts;
	return 0;
END;
$$ LANGUAGE PLPGSQL;

--read out nodes' longitudes and latitudes from relations' member ways
create or replace function ron(way_ids BIGINT[], out ndlmt INTEGER[], out ndxs INTEGER[], out ndys INTEGER[]) AS $$
DECLARE
	w_id BIGINT;
	snode_id BIGINT;
	enode_id BIGINT;
	nd_ids BIGINT[] ;

	sw_ndxs INTEGER[];
	sw_ndys INTEGER[];
	ndx INTEGER = 1;
BEGIN
	foreach w_id in ARRAY way_ids loop

		--retrieve nodes' coordinates , which are longitudes and latitudes ,  from each way
		select array_agg(swns.lon order by swns.seq), array_agg(swns.lat order by swns.seq) into sw_ndxs, sw_ndys from (select cns.longitude as lon, cns.latitude as lat, wns.sequence_id as seq from current_nodes cns inner join(select * from way_nodes where way_id = w_id) wns on cns.id = wns.node_id) swns;

		--raise info '%', sw_ndxs;

		--subscripts to the first longitudes and latitudes of each way
		ndlmt = ndlmt || ndx;
		--concatenate the longitudes and latitudes to a giant array.
		ndxs = ndxs || sw_ndxs ;
		ndys = ndys || sw_ndys ;
		ndx = ndx + array_length(sw_ndxs, 1);

	end loop;

	ndlmt = ndlmt || array_length(ndxs, 1) + 1 ;

		-- the following statement can't guarantee the order of the nodes
--  select array_agg(ns) into nds from current_nodes ns inner join unnest(nd_ids) idc on ns.id = idc;

END;
$$ LANGUAGE PLPGSQL;

-- sort outer ring of the relation that is a 'boundary' or 'multipolygon'
	--read out ways, pack its id into an array 'w_ids'
	--read out endpoints of ways and pack into an array 'nds'
create or replace function rws(rel_id BIGINT, out w_ids BIGINT[], out nds current_nodes[]) as $$
DECLARE

	way_limit INTEGER := 0;
	first_way relation_members;
	way_num INTEGER := 0;

	isFound boolean := false;
	isCloseFormed boolean := false;

	fnode current_nodes;
BEGIN
	select count(*) into way_limit from relation_members where relation_id = rel_id and member_role='outer' and member_type = 'Way';
	-- fetch the first way with role as 'outer'
	select * into first_way from relation_members where relation_id = rel_id and member_role = 'outer' and member_type = 'Way' order by sequence_id limit 1;

	-- the terminate condition is that the way_num exceed the way_limit or we reach the way which has already been in the way_id array.
	if FOUND then 
		way_num = 1;
		-- feed in the first way_id
		w_ids = w_ids || first_way.member_id;
		select * into fnode from current_nodes where id in (select node_id from way_nodes where way_id = first_way.member_id and sequence_id = 1);
		-- feed in the first node of the first way(frontier way)
		nds = nds || fnode;

		select * into fnode from current_nodes where id in (select node_id from way_nodes where way_id = first_way.member_id order by sequence_id DESC limit 1) ;
		-- feed in the last node of the first way(frontier way)
		nds = nds || fnode;

		-- make the way to progress to find the successive way
		while way_num <= way_limit and not isCloseFormed loop
			select seq_way_ids, seq_nodes, isin, iscf into w_ids, nds, isFound, isCloseFormed from sr(rel_id, w_ids, nds);
			if isCloseFormed then
				exit;
			end if;

			--raise info 'the w_ids has % elements, the nds has % elements, the way number is %: ', array_length(w_ids, 1), array_length(nds, 1), way_limit;
			--raise info 'the last of w_ids : % ; the last of nds : % ; ',  w_ids[array_length(w_ids, 1)], nds[array_length(nds, 1)];

			-- search the next way in the whole database.
			select seq_way_ids, seq_nodes, isin, iscf into w_ids, nds, isFound, isCloseFormed from sw(rel_id, w_ids, nds);

			--raise info 'after sw, the last of w_ids : % ; the last of nds : % ; ',  w_ids[array_length(w_ids, 1)], nds[array_length(nds, 1)];
		
			if isCloseFormed then
				exit;
			elsif not isFound then
				w_ids = null;
				return ;
			end if;

			way_num = array_length(w_ids, 1);

			--raise info 'the next way is %, the frontier node is %', next_id, frontier_node_id;
		end loop;

		--raise info 'the first node of the relation pivot node: %', nds[1];
		--raise info '% : the last node of the relation pivot node: %', array_length(nds, 1), nds[array_length(nds, 1)];
		-- add the last node to form closed ring;
		--raise info 'The whole array of the endpoints of relations way is : %', nds;
	else 
		-- this is not a usual case that there is no way as the 'outer' 'way' of the relation
		--raise info 'not found the first_way of relation : %', rel_id;
		w_ids = null;
		return ;
	end if;
END;
$$ LANGUAGE PLPGSQL;

-- find the connected way using the relations member.
	-- search relation
create or replace function sr(rel_id BIGINT, INOUT seq_way_ids BIGINT[], INOUT seq_nodes current_nodes[], OUT isin BOOLEAN, OUT iscf BOOLEAN) AS $$
DECLARE
	-- frontier node
	fnode current_nodes;
	-- first way's id in relation
	first_id BIGINT;
	-- frontier way's id
	fway_id BIGINT;
	-- array of ways following the frontier way
	w_ids BIGINT[];
	w_num INTEGER;
	i INTEGER := 1;
	-- frontier candidate first node
	fcsnode current_nodes; 
	-- frontier candidate last node
	fcenode current_nodes;
	
	isdebug boolean = false;

BEGIN

	-- boolean value of whether endpoint nodes of a relation forms closed ring
	iscf = false;
	-- the frontier node which is the last node in the nodes array 
	fnode = seq_nodes[array_length(seq_nodes, 1)];
	--raise info 'the sr frontier node is : %' , fnode;
	first_id = seq_way_ids[1];
	fway_id = seq_way_ids[array_length(seq_way_ids, 1)];
	select array_agg(member_id order by sequence_id) into w_ids from relation_members where relation_id = rel_id and sequence_id >= (select sequence_id from relation_members where relation_id = rel_id and member_id = fway_id and member_type='Way' limit 1) and member_type = 'Way' and member_role = 'outer' ;
	if FOUND then
		w_num = array_length(w_ids, 1);

		while i < w_num loop

			select * into fcsnode from current_nodes where id = (select node_id from way_nodes where way_id = w_ids[i + 1] and sequence_id = 1);
			select * into fcenode from current_nodes where id = (select node_id from way_nodes where way_id = w_ids[i + 1] order by sequence_id DESC limit 1);

			if w_ids[i + 1] = fway_id then
				return;
			end if;

			if fcsnode.id = fnode.id then

				if w_ids[i + 1] = first_id then iscf = true; return ; end if;

				seq_nodes = seq_nodes || fcenode;
				seq_way_ids = seq_way_ids || w_ids[i + 1];
			elsif fcenode.id = fnode.id then

				if w_ids[i + 1] = first_id then iscf = true; return ; end if;

				seq_nodes = seq_nodes || fcsnode;
				seq_way_ids = seq_way_ids || w_ids[i + 1];
			else
				-- can't proceed to the next way
				exit;
			end if;

			i = i + 1;
		end loop;
		isin = true;
	else 
		isin = false;
	end if;

END;
$$ LANGUAGE PLPGSQL;

-- get the next way id if not loop 
	-- search ways
create or replace function sw(rel_id BIGINT, INOUT seq_way_ids BIGINT[], INOUT seq_nodes current_nodes[], OUT isin BOOLEAN, OUT iscf BOOLEAN) AS $$
DECLARE
	--frontier node
	fnode current_nodes;
	--frontier way_id
	fway_id BIGINT;
	--frontier candidate way_id
	fcway_id BIGINT;
	--frontier candidate way's first node ;
	fcsnode current_nodes;
	--frontier candidate way's last node ;
	fcenode current_nodes;

	isdebug boolean = false;
BEGIN

	iscf = false;
	fway_id = seq_way_ids[array_length(seq_way_ids, 1)];
	fnode = seq_nodes[array_length(seq_nodes,1)];

	-- there are following steps to search for the target way_id and node
		-- set the pool as the ways with 'outer' role in a relation
		-- select the specific way which contains the frontier node excpet the frontier way itself from the pool
		-- select the first and last node from the specific way

	select way_id into fcway_id from way_nodes where node_id = fnode.id and way_id <> fway_id and way_id in (select member_id from relation_members where relation_id = rel_id and member_type = 'Way' and member_role = 'outer');

	if FOUND then

		--raise info 'the frontier_id : % ;', fcway_id;

		if fcway_id = seq_way_ids[1] then
			iscf = true;
			return;
		end if;

		select * into fcsnode from current_nodes where id = (select node_id from way_nodes where way_id = fcway_id and sequence_id = 1);
		select * into fcenode from current_nodes where id = (select node_id from way_nodes where way_id = fcway_id order by sequence_id DESC limit 1);

		--raise info 'fcsnode in relation : %, fcenode in relation : %', fcsnode, fcenode; 

		if not fcenode is null then
			if fcsnode.id = fnode.id then
				-- this is the usual successful case
				seq_nodes = seq_nodes || fcenode;
				seq_way_ids = seq_way_ids || fcway_id;
				isin = true;
			elsif fcenode.id = fnode.id then
				seq_nodes = seq_nodes || fcsnode;
				seq_way_ids = seq_way_ids || fcway_id;
				isin = true;
			else
				isin = false;
				-- the two ways share common nodes don't join on their endpoints
					-- we can search the way in a larger pool -- all the ways in the database
			end if;
		end if;

	else 
		
		--how about when there are many way meet the condition?
			-- we dont permit the closed way to be a frontier candidate
		--select way_id into fcway_id from way_nodes where node_id = fnode.id and way_id <> fway_id;

		select way_id into fcway_id from way_nodes where way_id in (select distinct way_id from way_nodes where node_id = fnode.id and way_id <> fway_id) group by way_id having count(distinct node_id) = max(sequence_id);

		if FOUND then

			if fcway_id = seq_way_ids[1] then
				iscf = true;
				return;
			end if;

			select * into fcsnode from current_nodes where id = (select node_id from way_nodes where way_id = fcway_id and sequence_id = 1);
			select * into fcenode from current_nodes where id = (select node_id from way_nodes where way_id = fcway_id order by sequence_id DESC limit 1);

			--raise info 'fcsnode in database : %, fcenode in database : %', fcsnode, fcenode; 

			if not fcenode is null then
				if fcsnode.id = fnode.id then
					seq_nodes = seq_nodes || fcenode;
					seq_way_ids = seq_way_ids || fcway_id;
					isin = true;
				elsif fcenode.id = fnode.id then
					seq_nodes = seq_nodes || fcsnode;
					seq_way_ids = seq_way_ids || fcway_id;
					isin = true;
				else
					--raise info 'failed retrieve the common endnodes in two ways : % -- with commond nodes : %',fcway_id,  fnode.id; 
					isin = false;
				end if;
			end if;

		else
			-- there is no successive way beyond the last way, so it is an error
			isin = false;
			--raise info 'No other way contains frontier node : %', fnode.id;
		end if;
	end if;

END;
$$ LANGUAGE PLPGSQL;

--calculate the error of each endpoints of the relations' member ways
	-- simplify the relation 
create or replace function s(rel_id BIGINT, sorted_way_ids BIGINT[], nds current_nodes[], rndlmt INTEGER[], rndxs INTEGER[], rndys INTEGER[], i INTEGER, j INTEGER, n_path CHAR(8), p real) returns real AS $$
DECLARE
	pool INTEGER;
	k INTEGER;
	cur_dis real = -1.0;
	max_dis real = -1.0;
	pool_max_dis real = -1.0;
	max_pos INTEGER = i + 1;
	geohash_tile CHAR(8);
	cur_id INTEGER;
	max_lon DOUBLE PRECISION;
	max_lat DOUBLE PRECISION;
	x1 DOUBLE PRECISION;
	y1 DOUBLE PRECISION;
	x2 DOUBLE PRECISION;
	y2 DOUBLE PRECISION;
	x3 DOUBLE PRECISION;
	y3 DOUBLE PRECISION;
	left_error real := -1.0;
	right_error real := -1.0;
	max_child_error real := 1.0;
	cur_error real := -1.0;
	-- add the four column to the relation_trees table
	om_id BIGINT;
	cm_id BIGINT;
	error_from_ways REAL := -1;
	xarr INTEGER[];
	yarr INTEGER[];
BEGIN
	-- return the way error 
	if i + 1 >= j then
		select error into cur_error from way_trees where way_id = sorted_way_ids[i] and path = '0';
		if cur_error is null then return 0; end if;
		return cur_error; 
	end if;

	x1 = nds[i].longitude::DOUBLE PRECISION / 10000000.0;
	y1 = nds[i].latitude::DOUBLE PRECISION / 10000000.0;
	x2 = nds[j].longitude::DOUBLE PRECISION / 10000000.0;
	y2 = nds[j].latitude::DOUBLE PRECISION / 10000000.0;

	pool = ((j - i - 1) * p)::INTEGER;
	k = i + 1;

	while k < j loop
		x3 = nds[k].longitude::DOUBLE PRECISION / 10000000.0;
		y3 = nds[k].latitude::DOUBLE PRECISION / 10000000.0;

		cur_dis = seg2pt_c(x1,y1,x2,y2,x3,y3);
		if cur_dis > max_dis then
			max_dis = cur_dis;
		end if;

		if abs(k - (j + i) / 2) <= pool then
			if cur_dis > pool_max_dis then
				pool_max_dis = cur_dis;
				max_pos = k;
				max_lon = x3;
				max_lat = y3;
			end if;
		end if;
		k = k + 1;
	end loop;

	--raise info 'The divide point of the line is %', max_pos;
	left_error = s(rel_id, sorted_way_ids, nds, rndlmt, rndxs, rndys, i, max_pos, lc(n_path), p);
	right_error = s(rel_id, sorted_way_ids, nds, rndlmt, rndxs, rndys, max_pos, j, rc(n_path), p);

	if left_error > right_error then
		max_child_error = left_error;
	else
		max_child_error = right_error;
	end if;

	if max_dis > max_child_error then
		cur_error = max_dis;
	else
		cur_error = max_child_error * 1.1;
	end if;

	-- check the way error 
	xarr = subarray(rndxs, rndlmt[i], rndlmt[j] - rndlmt[i]);
	yarr = subarray(rndys, rndlmt[i], rndlmt[j] - rndlmt[i]);

	error_from_ways = maxdis_c(xarr, yarr, nds[i].longitude, nds[i].latitude, nds[j].longitude, nds[j].latitude);
	if error_from_ways > cur_error then
		cur_error = error_from_ways;
	end if;

	geohash_tile = gcode_c(max_lon, max_lat, cur_error);

	-- opening member id and closing member id of the selected node
	om_id = sorted_way_ids[max_pos];
	cm_id = sorted_way_ids[max_pos - 1];

	--insert into relation_trees(relation_id, node_id, error, path, subsize, tile) values(rel_id, nds[max_pos].id, cur_error, n_path, j - i - 1, geohash_tile);
	insert into relation_trees(relation_id, node_id, opening_member_id, closing_member_id, opening_member_type, closing_member_type, error, path, subsize) values (rel_id, nds[max_pos].id, om_id, cm_id, 'Way', 'Way', cur_error, n_path, j - i - 1);

	-- update the node_vis_errors table if the error is a new maximum
	update node_vis_errors set error = cur_error, tile = geohash_tile where node_id = nds[max_pos].id and error < cur_error;

	return cur_error;
END;

$$ LANGUAGE PLPGSQL;

-- return the path of the left child
CREATE OR REPLACE FUNCTION lc(path char(8)) RETURNS CHAR(8) AS $$
DECLARE

	lchild_codes char[] := array['5', '6', '9', '8', '=', '>', 'A', '<', 'E', 'F', 'I',
																'H', 'M', 'N', 'Q', 'D', 'U', 'V', 'Y', 'X', ']', 
																'^', 'a', '\', 'e', 'f', 'i', 'h', 'm', 'n', 'q'];

	--'-- above is the tree nodes' path code

	retval CHAR(8) := '';
	tail_ch CHAR := '';
BEGIN
	tail_ch = right(path, 1);
	tail_ch = lchild_codes[(ascii(tail_ch) - 52) / 2];

	retval = left(path, -1) || tail_ch ;
	if (ascii(tail_ch) - 52) % 2 = 1 then retval = retval || 'T'; end if;
	return retval;
END;
$$ LANGUAGE PLPGSQL;

-- NO (5.2)
	-- return the path of the right child
CREATE OR REPLACE FUNCTION rc(path CHAR(8)) RETURNS CHAR(8) AS $$
DECLARE
	rchild_codes char[] := array['7', ':', ';', '@', '?', 'B', 'C', 'L', 'G', 'J', 'K',
																'P', 'O', 'R', 'S', 'd', 'W', 'Z', '[', '`', '_', 
																'b', 'c', 'l', 'g', 'j', 'k', 'p', 'o', 'r', 's'];

	--'-- above is the tree nodes' path code

	retval CHAR(8) := '';
	tail_ch CHAR := '';
BEGIN
	tail_ch = right(path, 1);
	tail_ch = rchild_codes[(ascii(tail_ch) - 52) / 2 ];

	retval = left(path, -1) || tail_ch;
	if (ascii(tail_ch) - 52) % 2 = 1 then retval = retval || 'T'; end if;
	return retval;
END;
$$ LANGUAGE PLPGSQL;

--insert the two endpoints of a relation into the relation_trees tables 
create or replace function insert_relends(rel_id BIGINT, root_error real, opening_way_id BIGINT, closing_way_id BIGINT, rel_nodes_num INTEGER) RETURNS INTEGER AS $$
DECLARE
	rel_snode current_nodes;
	rel_enode current_nodes;
	rel_snode_tile CHAR(8);
	rel_enode_tile CHAR(8);
	rel_error real := root_error * 1.1;
BEGIN
	select * into rel_snode from current_nodes where id = (select node_id from way_nodes where way_id = opening_way_id and sequence_id = 1);
	select * into rel_enode from current_nodes where id = (select node_id from way_nodes where way_id = closing_way_id order by sequence_id DESC limit 1);

	rel_snode_tile = gcode_c(rel_snode.longitude::DOUBLE PRECISION / 10000000.0, rel_snode.latitude::DOUBLE PRECISION / 10000000.0, rel_error);
	rel_enode_tile = gcode_c(rel_enode.longitude::DOUBLE PRECISION / 10000000.0, rel_enode.latitude::DOUBLE PRECISION / 10000000.0, rel_error);

	insert into relation_trees(relation_id, node_id, opening_member_id, closing_member_id, opening_member_type, closing_member_type, error, path, subsize) values(rel_id, rel_snode.id, opening_way_id, closing_way_id, 'Way', 'Way', rel_error, '0', rel_nodes_num);
	insert into relation_trees(relation_id, node_id, opening_member_id, closing_member_id, opening_member_type, closing_member_type, error, path, subsize) values(rel_id, rel_enode.id, opening_way_id, closing_way_id, 'Way', 'Way', rel_error, 't', rel_nodes_num);

	--update the node's error with new maximum
	update node_vis_errors set error = rel_error, tile = rel_snode_tile where node_id = rel_snode.id and error < rel_error ;
	update node_vis_errors set error = rel_error, tile = rel_enode_tile where node_id = rel_enode.id and error < rel_error ;

	return 0;
END;
$$ LANGUAGE PLPGSQL;

-- obselete function, we have inline the code into the simplify function
-- computer way node's errors against the relation's way-endpoints
create or replace function cwe(w_ids BIGINT[], rndlmt INTEGER[], rndxs INTEGER[], rndys INTEGER[], i INTEGER, j INTEGER, x1 INTEGER, y1 INTEGER, x2 INTEGER, y2 INTEGER) returns REAL AS $$
DECLARE
	--between_nodes rwnodes[];
	--bnode rwnodes;
	--between_ways_ids BIGINT[];

	xarr INTEGER[] ;
	yarr INTEGER[] ;

	k INTEGER := i;
	max_dis REAL := -1;
	cur_dis REAL := -1;

	subarr_s INTEGER;
	subarr_e INTEGER;

BEGIN
	-- select all the nodes between the ith way and (j-1)th way

	/*
	if i + 1 >= j then return max_dis ; end if;
	while k < j loop
		between_ways_ids = between_ways_ids || w_ids[k];
		k = k + 1;
	end loop;
	*/

	--select array_agg(rns.*) into between_nodes from rwnodes rns where rns.way_id = ANY(between_ways_ids);

	--select array_agg(rns.lon order by rns.way_id, rns.node_id) into xarr from rwnodes rns where rns.way_id = ANY(between_ways_ids);
	--select array_agg(rns.lat order by rns.way_id, rns.node_id) into yarr from rwnodes rns where rns.way_id = ANY(between_ways_ids);


	--select array_agg(rns.lon), array_agg(rns.lat) into xarr, yarr from rwnodes rns where rns.way_id = ANY(between_ways_ids);

	subarr_s = rndlmt[i];
	subarr_e = rndlmt[j];

	xarr = subarray(rndxs, subarr_s, subarr_e - subarr_s);
	yarr = subarray(rndys, subarr_s, subarr_e - subarr_s);

	max_dis = maxdis_c(xarr, yarr, x1, y1, x2, y2);

	/*
	
	foreach bnode in array between_nodes loop
		cur_dis = seg2pt_c(x1, y1, x2, y1, bnode.lon / 10000000.0, bnode.lat / 10000000.0);
		if cur_dis > max_dis then
			max_dis = cur_dis;
		end if;
	end loop;
	*/
	return max_dis;
END;
$$ LANGUAGE PLPGSQL;

----------relation.sql----------------------------------------------------------------


----------tree_operations.sql----------------------------------------------------------------
-- NO. (6)
	-- return ancestors' pathes of a single node using its path
CREATE OR REPLACE FUNCTION get_ancestors(node_path char(8)) RETURNS char(8)[] AS $$

DECLARE

	ancestor_table char[][] := ARRAY[['6','8','<','D','T'], ['8','<','D','T', ''], ['6','8','<','D','T'], ['<','D','T', '', ''], [':','8', '<','D','T'], ['8','<','D','T', ''],  [':', '8', '<','D','T'], ['D', 'T', '', '', ''], ['>', '@', '<', 'D', 'T'], ['@', '<', 'D', 'T', ''], ['>', '@', '<', 'D', 'T'], ['<','D','T', '', ''], ['B', '@', '<', 'D', 'T'], ['@', '<', 'D', 'T', ''], ['B', '@', '<', 'D', 'T'], ['T', '', '', '', ''], ['F', 'H', 'L', 'D', 'T'], ['H', 'L', 'D', 'T', ''], ['F', 'H', 'L', 'D', 'T'], ['L', 'D', 'T', '', ''],
																		['J', 'H', 'L', 'D', 'T'], ['H', 'L', 'D', 'T', ''], ['J', 'H', 'L', 'D', 'T'], ['D', 'T', '', '', ''], ['N', 'P', 'L', 'D', 'T'],
																		['P', 'L', 'D', 'T', ''], ['N', 'P', 'L', 'D', 'T'], ['L', 'D', 'T', '', ''], ['R', 'P', 'L', 'D', 'T'], ['P', 'L', 'D', 'T', ''],
																		['R', 'P', 'L', 'D', 'T'], ['', '', '', '', ''], ['V', 'X', '\', 'd', 'T'], ['X', '\', 'd', 'T', ''], ['V', 'X', '\', 'd', 'T'],
																		['\', 'd', 'T', '', ''], ['Z', 'X', '\', 'd', 'T'], ['X', '\', 'd', 'T', ''], ['Z', 'X', '\', 'd', 'T'], ['d', 'T', '', '', ''],
																		['^', '`', '\', 'd', 'T'], ['`', '\', 'd', 'T', ''], ['^', '`', '\', 'd', 'T'], ['\', 'd', 'T', '', ''], ['b', '`', '\', 'd', 'T'],
																		['`', '\', 'd', 'T', ''], ['b', '`', '\', 'd', 'T'], ['T', '', '', '', ''], ['f', 'h', 'l', 'd', 'T'], ['h', 'l', 'd', 'T', ''],
																		['f', 'h', 'l', 'd', 'T'], ['l', 'd', 'T', '', ''], ['j','h', 'l', 'd', 'T' ], ['h', 'l', 'd', 'T', ''], ['j','h', 'l', 'd', 'T'],
																		['d', 'T', '', '', ''], ['n', 'p', 'l', 'd', 'T'], ['p', 'l', 'd', 'T', ''], ['n', 'p', 'l', 'd', 'T'], ['l', 'd', 'T', '', ''],
																		['r', 'p', 'l', 'd', 'T'], ['p', 'l', 'd', 'T', ''], ['r', 'p', 'l', 'd', 'T']];
--' above is the table to make craft a const time ancestor lookup

	len SMALLINT := 1;
	count SMALLINT := 1;

	ch char := '';
	chc SMALLINT := 32;
	i SMALLINT := 1;
	ch2 CHAR := '';

	prefix char(8) := '';
	can char(8) := '';

	ret CHAR(8)[] ;
BEGIN
	len = length(node_path); 
	WHILE count < len + 1 LOOP

		ch = substring(node_path from count for 1);
		chc = ascii(ch) - 52;

		i = 5;
		WHILE i > 0 LOOP
			
			ch2 = ancestor_table[chc][i];

			IF ch2 <> '' THEN
					
				--RAISE NOTICE '%', ch2;
				can = prefix || ch2 ;
				ret = ret || can ;

			END IF;

			i = i - 1;
		END LOOP;
		count = count + 1;
		prefix = prefix || ch;
	END LOOP;

	return ret;

END;
$$ LANGUAGE PLPGSQL;

--NO.(5.1)
CREATE OR REPLACE FUNCTION lchild_path(path char(8)) RETURNS CHAR(8) AS $$
DECLARE

	lchild_codes char[] := array['5', '6', '9', '8', '=', '>', 'A', '<', 'E', 'F', 'I',
																'H', 'M', 'N', 'Q', 'D', 'U', 'V', 'Y', 'X', ']', 
																'^', 'a', '\', 'e', 'f', 'i', 'h', 'm', 'n', 'q'];

	--'-- above is the tree nodes' path code

	retval CHAR(8) := '';
	tail_ch CHAR := '';
BEGIN
	tail_ch = right(path, 1);
	tail_ch = lchild_codes[(ascii(tail_ch) - 52) / 2];

	retval = left(path, -1) || tail_ch ;
	if (ascii(tail_ch) - 52) % 2 = 1 and length(retval) < 8 then
		retval = retval || 'T';
	end if;
	return retval;
END;
$$ LANGUAGE PLPGSQL;

-- NO (5.2)
CREATE OR REPLACE FUNCTION rchild_path(path CHAR(8)) RETURNS CHAR(8) AS $$
DECLARE
	rchild_codes char[] := array['7', ':', ';', '@', '?', 'B', 'C', 'L', 'G', 'J', 'K',
																'P', 'O', 'R', 'S', 'd', 'W', 'Z', '[', '`', '_', 
																'b', 'c', 'l', 'g', 'j', 'k', 'p', 'o', 'r', 's'];

	--'-- above is the tree nodes' path code

	retval CHAR(8) := '';
	tail_ch CHAR := '';
BEGIN
	tail_ch = right(path, 1);
	tail_ch = rchild_codes[(ascii(tail_ch) - 52) / 2 ];

	retval = left(path, -1) || tail_ch;
	if (ascii(tail_ch) - 52) % 2 = 1 and length(retval) < 8 then
		retval = retval || 'T';
	end if;
	return retval;
END;
$$ LANGUAGE PLPGSQL;

-- some global parameters of the extension : Delta, Gamma, Proportion
	-- the Delta and gamma are from the paper: Hirai, Y.; Yamamoto, K. (2011). "Balancing weight-balanced trees" (PDF). J. Functional Programming 21 (3): 287. doi:10.1017/S0956796811000104
		-- Delta := 3	Which means the allowed proportion of the size of two subtree are 7.5 : 2.5
		-- Gamma := 2 
		-- proportion := 0.1; which means the allowed size of two subtree are 6 : 4
CREATE OR REPLACE FUNCTION isBalanced(lsubsize INTEGER, rsubsize INTEGER) RETURNS BOOLEAN AS $$
DECLARE
	Delta SMALLINT := 3;
BEGIN
	return  ((lsubsize + 1) * Delta > (rsubsize + 1)) AND ((rsubsize + 1) * Delta > (lsubsize + 1)) ;
END;
$$ LANGUAGE PLPGSQL;

-- get the lowest common ancestor node from an array of nodes' path
	-- return the LCA node's path
create or replace function LCA(paths CHAR(8)[]) returns CHAR(8) AS $$
declare
	i INTEGER;
	j INTEGER;
	len INTEGER;
	min_len INTEGER := 8;
	path_it CHAR(8);
	common_prefix CHAR(8) = '';
	
	lca_char CHAR ;
	diverse_chars CHAR(8)[];
begin
	foreach path_it in ARRAY paths loop
		len = length(path_it);
		if len < min_len then
			min_len = len;
		end if;
	end loop;

	common_prefix = left(paths[1], min_len);
	foreach path_it in ARRAY paths loop
		j = min_len;
		while left(path_it, j) <> left(common_prefix, j) loop
			j = j - 1;
		end loop;
		min_len = j;
		if min_len = 0 then exit ; end if;
	end loop;

	common_prefix = left(common_prefix, min_len);
	-- if the last letter is not leaf, then no need to attach lca letter
	if min_len <> 0 then
		if (ascii(right(common_prefix, 1)) - 52) % 2 <> 1 then
			return common_prefix;
		end if;
	end if;

	-- here to compute the first diverse character
	foreach path_it in ARRAY paths loop
		diverse_chars = diverse_chars || substring(path_it from min_len + 1 for 1)::CHAR(8);
	end loop;

	lca_char = LCAchr(diverse_chars);
	return common_prefix || lca_char;
end;
$$ LANGUAGE PLPGSQL;

-- get the lowest common ancestor node from an array of nodes' path
	-- return the LCA node's path
CREATE OR REPLACE FUNCTION LCAchr(paths CHAR(8)[]) RETURNS CHAR(8) AS $$

DECLARE
	path CHAR(8);
	ch CHAR ;
	lca_ch CHAR(8) := '';

	lca_code int := 32;
	path_code int := 32;
	last_1bit int := 1;
	lca_1bit int := 1;
	i INTEGER := 1;
BEGIN
	foreach path in ARRAY paths LOOP
		ch = right(path, 1) ;
		if lca_ch = '' then lca_ch = ch; end if;
		if ch = 'T' then return ch ; end if;
		lca_code = ascii(lca_ch) - 52;
		path_code = ascii(ch) - 52;
		last_1bit = last1bit(path_code);
		lca_1bit = last1bit(lca_code);
		if last_1bit < lca_1bit then
			lca_code = setbit(lca_code, last_1bit, 1);
			lca_code = bitmask(lca_code, last_1bit + 1, 0);
			lca_ch = chr(lca_code + 52);
			lca_1bit = last_1bit;
		end if;
		i = 1;
		while i < lca_1bit loop
			if getbit(path_code, i) <> getbit(lca_code, i) then
				lca_code = setbit(lca_code, i, 1);
				lca_code = bitmask(lca_code, i + 1, 0);
				lca_ch = chr(lca_code + 52);
				exit;
			end if;
			if lca_ch = 'T' then return lca_ch;  end if;
			i = i + 1;
		end loop;
	END LOOP ;
	return lca_ch;
END;

$$ LANGUAGE PLPGSQL;

-- get the last 1 bit position ( 1 ~ 6) which means the depth of the node.
	--calculate the position of the rightmost bit of 1
	-- the rightmost bit is position 6
	--					 x x 1 2 3 4 5 6	
		--  byte:  0 0 0 0 0 0 0 0 
CREATE OR REPLACE FUNCTION last1bit(num int) RETURNS int AS $$ 
DECLARE
	j int := 0;
BEGIN
	while j < 6 loop
		if (num >> j) & X'01'::int > 0 then return 6 - j; end if;
		j = j + 1;
	end loop;
	return 0;
END;
$$ LANGUAGE PLPGSQL;

-- get the specific position(1 ~ 6) bit 
CREATE OR REPLACE FUNCTION getbit(num int, ind int) RETURNS int AS $$
BEGIN
	return (num >> (6 - ind)) & X'01'::INTEGER ;
END;
$$ LANGUAGE PLPGSQL;

-- set the specific position(1~6) bit to 1 or 0
CREATE OR REPLACE FUNCTION setbit(num int, ind int, v int) RETURNS int AS $$ 
BEGIN
	if v > 0 then
		return num | (X'01' << (6 - ind))::INTEGER ;
	else 
		return num & (255 - (X'01' << (6 - ind))::INTEGER)::INTEGER ;
	end if;
END;
$$ LANGUAGE PLPGSQL;

-- set bit from index ind(inclusive) to 6 
	-- the bit can be 1 or 0
CREATE OR REPLACE FUNCTION bitmask(num int, ind int, v int) RETURNS int AS $$
DECLARE
	i int := 1;
	mask int := 255;
BEGIN
	if v = 0 then
		mask = mask << (7 - ind);
		return mask & num;
	else 
		mask = 255 - (mask << (7 - ind));
		return mask | num;
	end if;
END;
$$ LANGUAGE PLPGSQL;

----------tree_operations.sql----------------------------------------------------------------

----------geom_computations.sql----------------------------------------------------------------

-- the inputed nodes are in array instead of in refcursor as in simplify2  
CREATE OR REPLACE FUNCTION simplify(cur_way_id BIGINT, subline current_nodes[], i INTEGER, j INTEGER, p REAL, pth CHAR(8) ) RETURNS REAL AS $$

DECLARE
	k INTEGER := i + 1;

	cur_dis REAL := -1;
	max_dis REAL := -1;
	
	pool_max_dis REAL := -1;
	pool_max_pos INTEGER := i + 1;

	sta_node current_nodes%ROWTYPE;
	end_node current_nodes%ROWTYPE;
	cur_node current_nodes%ROWTYPE;

	lft_err REAL := -1;
	rgt_err REAL := -1;

	finalerror real := 0;
	existing_node way_trees%ROWTYPE;

	gtl CHAR(8) := '';
BEGIN

	IF i + 1 >= j THEN
		RETURN 0;
	END IF;

	sta_node = subline[i];
	end_node = subline[j];

	WHILE k < j LOOP

		cur_node = subline[k];
		cur_dis = seg2pt_c(sta_node.longitude / 10000000.0, sta_node.latitude / 10000000.0,
										 end_node.longitude / 10000000.0, end_node.latitude / 10000000.0, 
			 							 cur_node.longitude / 10000000.0, cur_node.latitude / 10000000.0);

		IF cur_dis > max_dis THEN
			max_dis = cur_dis;
		END IF;
		
		IF cur_dis > pool_max_dis THEN
			IF abs((i + j) / 2 - k) < (j - i - 1) * p THEN
				pool_max_dis = cur_dis;
				pool_max_pos = k;
			END IF;
		END IF;

		k = k + 1;
	END LOOP;
		
	--recursively call the simplify3 

	if lchild_path(pth) is null then 
		raise info 'the lchild of % is NULL!!', pth;
	end if;

	if rchild_path(pth) is null then
		raise info 'the rchild of % is NULL!!', pth;
	end if;
	lft_err = simplify(cur_way_id, subline, i, pool_max_pos, p, lchild_path(pth)); 
	rgt_err = simplify(cur_way_id, subline, pool_max_pos, j, p, rchild_path(pth)); 

	finalerror = lft_err;
	if rgt_err > finalerror then
		finalerror = rgt_err;
	end if;

	if max_dis > finalerror then 
		finalerror = max_dis ;
	else
		finalerror = finalerror * 1.1;
	end if;

	--encode the geohash as spatial index
	gtl = gcode_c(subline[pool_max_pos].longitude/10000000.0, subline[pool_max_pos].latitude/10000000.0, finalerror);

		-- if exist then update (in the case of ways editing, seq_replace, some node need to be udpated to ), insert otherwise
	select * into existing_node from way_trees where node_id = subline[pool_max_pos].id and way_id = cur_way_id ; -- and path = subline[pool_max_pos].path ;

	if FOUND then
		--
		update way_trees set (error, path, subsize) = (finalerror, pth, j - i - 1) where node_id = subline[pool_max_pos].id and way_id = cur_way_id and path=existing_node.path ; --and path = subline[pool_max_pos].path;
	else 
		insert into way_trees(way_id, node_id, error, path, subsize) values (cur_way_id, subline[pool_max_pos].id, finalerror, pth, j - i - 1);
	end if;

	-- update the node_vis_errors if the error is a new maximum
	update node_vis_errors set error = finalerror, tile = gtl where node_id = subline[pool_max_pos].id and error < finalerror;

	return finalerror;

END;

$$ LANGUAGE PLPGSQL ;

create or replace function pt2pt(x1 double precision, y1 double precision, x2 double precision, y2 double precision) returns real AS $$
DECLARE
	ret_value real := 0;

BEGIN
	ret_value = sqrt((x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2));
	return ret_value;
END;
$$ LANGUAGE PLPGSQL;

----------geom_computations.sql----------------------------------------------------------------


----------way_editing.sql----------------------------------------------------------------

--insert nodes sequence into a way
	-- can't handle the case that nodes sequence are inserted before the first node of way(node '0') or after the last node of way (node 't')
CREATE OR REPLACE FUNCTION seq_insertion(cur_way_id BIGINT, lnode_id BIGINT, rnode_id BIGINT, new_nodes_ids BIGINT[]) RETURNS way_trees AS $$
DECLARE
	lnode way_trees;
	rnode way_trees;

	lend_path CHAR(8) := '0';
	rend_path CHAR(8) := 't';

	parents_path CHAR(8)[] ;
	parents_num INTEGER ;
	new_seq_path CHAR(8) ; 
	diff INTEGER ;
	i INTEGER := 1;

	lsubsize INTEGER ;
	rsubsize INTEGER ;

	unbalanced_root CHAR(8) = '';

	subtree_nodes current_nodes[];
	rebuild_path CHAR(8) ;
	p real := 0.1;
	err real := -1;

	-- base line endpoints 
	lbase_nd current_nodes;
	rbase_nd current_nodes;
	-- introduced error from new node sequence 
	parent_tnode way_trees;
	parent_node current_nodes;
	new_err real := -1;
	new_tile char(8) := '';

	x1 double precision = 0;
	y1 double precision = 0;
	x2 double precision = 0;
	y2 double precision = 0;

	new_nodes current_nodes[];
	lbase_tnd way_trees;
	rbase_tnd way_trees;
	lbase_tnd_tile char(8);
	rbase_tnd_tile char(8);

	lnerror real := 0;

	parent_error real := 0;
	new_parent_error real := 0;
	new_parent_node current_nodes;
	new_parent_tile CHAR(8) = '';

BEGIN
	
	select * into lbase_nd from current_nodes where id = (select node_id from way_trees where way_id = cur_way_id and path = '0');
	select * into rbase_nd from current_nodes where id = (select node_id from way_trees where way_id = cur_way_id and path = 't');
	
	-- the path condition to guard the case of ring with same first and last node
		-- guard the case lnode or rnode is node '0' or node 't'
	select * into lnode from way_trees where way_id = cur_way_id and node_id = lnode_id and path < 't';
	select * into rnode from way_trees where way_id = cur_way_id and node_id = rnode_id and path > '0';

	--raise info 'new_nodes_ids : % ', new_nodes;
	select array_agg(cns.* order by Z.ind) into new_nodes from current_nodes cns inner join (select *, new_nodes_ids[ind] as nid from (select generate_subscripts(new_nodes_ids, 1) as ind)X )Z on cns.id = Z.nid;

	--raise info 'left node path: % ; right node path: %', lnode.path, rnode.path;
	--raise info 'new_nodes: % ', new_nodes;

	-- handle the corner case where lnode.path = '0' and rnode.path = 't';
	if lnode.path = '0' and rnode.path = 't' then
		-- as a new line contruction
		err = simplify(cur_way_id, new_nodes, 1, array_length(new_nodes, 1), p, 'T' );
		return null;
	end if;

	x1 = lbase_nd.longitude / 10000000.0 ; y1 = lbase_nd.latitude / 10000000.0;
	x2 = rbase_nd.longitude / 10000000.0 ; y2 = rbase_nd.latitude / 10000000.0;

	if lnode.subsize < rnode.subsize then
		parents_path = get_ancestors(lnode.path);
		parents_path = parents_path || lnode.path ;
		new_seq_path = rchild_path(lnode.path);
	else
		parents_path = get_ancestors(rnode.path);
		parents_path = parents_path || rnode.path ;
		new_seq_path = lchild_path(rnode.path);
	end if;	

	diff = array_length(new_nodes_ids, 1);
	raise info 'the length of the node seq is %', diff;

	parents_path = parents_path || new_seq_path;
	parents_num = array_length(parents_path, 1);

	raise info 'the parents_path is %', parents_path;

	-- top-down traversal of the tree to test each nodes' balance in the path of the inserted nodes
		-- test the balance with the changed child size 
	while i < parents_num loop
	-- from i to parents_num - 1, compute the error from the ith parents to the inserted node sequence.
		if parents_path[i + 1] < parents_path[i] then
			-- calculate the new error for the node with path parents_path[i]
			new_err = max_dis_from_seq(new_nodes, x1, y1, x2, y2);
			
			select subsize into lsubsize from way_trees where way_id = cur_way_id and path = parents_path[i + 1];
			select subsize into rsubsize from way_trees where way_id = cur_way_id and path = rchild_path(parents_path[i]);
			if lsubsize is null then lsubsize = 0 ; end if;
			if rsubsize is null then rsubsize = 0 ; end if;
			lsubsize = lsubsize + diff;

			select * into rbase_nd from current_nodes where id = (select node_id from way_trees where way_id = cur_way_id and path = parents_path[i]);
			x2 = rbase_nd.longitude / 10000000.0 ; y2 = rbase_nd.latitude / 10000000.0 ;
		else 
			select subsize into lsubsize from way_trees where way_id = cur_way_id and path = lchild_path(parents_path[i]);
			select subsize into rsubsize from way_trees where way_id = cur_way_id and path = parents_path[i + 1];
			if lsubsize is null then lsubsize = 0 ; end if;
			if rsubsize is null then rsubsize = 0 ; end if;
			rsubsize = rsubsize + diff;

			select * into lbase_nd from current_nodes where id = (select node_id from way_trees where way_id = cur_way_id and path = parents_path[i]);
			x1 = lbase_nd.longitude / 10000000.0 ; y2 = lbase_nd.latitude / 10000000.0;
		end if;

		-- set the left end and right end of the current subtree;
			-- set the left end and right by check the parent's path with the current node's path
		if i - 1 > 0 then
			if parents_path[i] < parents_path[i - 1] then
				rend_path = parents_path[i - 1];
			else 
				lend_path = parents_path[i - 1];
			end if;
		end if;

		--raise info 'lsubsize is % ; rsubsize is %', lsubsize, rsubsize;
		if not isBalanced(lsubsize, rsubsize) then 
			unbalanced_root = parents_path[i];
			exit;
		end if;

		select * into parent_tnode from way_trees where way_id = cur_way_id and path = parents_path[i];
		select * into parent_node from current_nodes where id = parent_tnode.node_id;

		-- update the parent_tnode's error(possible) subsize, and tile(possible) ;
		if new_err > parent_tnode.error then
			new_tile = gcode_c(parent_node.longitude / 10000000.0, parent_node.latitude / 10000000.0, new_err);
			update way_trees set (error, subsize) = (new_err, parent_tnode.subsize + diff) where way_id = cur_way_id and path = parents_path[i];
			update node_vis_errors set error = new_err, tile = new_tile where node_id = parent_node.id and error < new_err;
		else
			update way_trees set (subsize) = (parent_tnode.subsize + diff) where way_id = cur_way_id and path = parents_path[i];
			update node_vis_errors set error = new_err, tile = new_tile where node_id = parent_node.id and error < new_err;
		end if;

		i = i + 1;
	end loop;

	--raise info 'unbalanced_root_path is %', unbalanced_root;
	-- wether or not the unbalanced_root is not found
	if unbalanced_root = '' then
		subtree_nodes = fabricate_seq(cur_way_id, lnode.path, lnode.path, rnode.path, rnode.path, new_nodes_ids);
		rebuild_path = new_seq_path;
	else 
		subtree_nodes = fabricate_seq(cur_way_id, lend_path, lnode.path, rnode.path, rend_path, new_nodes_ids);
		rebuild_path = unbalanced_root;
	end if;

	-- using simplify to rebuild a subtree
	err = simplify(cur_way_id, subtree_nodes, 1, array_length(subtree_nodes, 1), p , rebuild_path);
	new_parent_error = err;

	-- update the error upwards the path from the point where subtree are rebuild
	while i > 1 loop

		select error into parent_error from way_trees where way_id = cur_way_id and path = parents_path[i - 1];
		if new_parent_error > parent_error then
			-- update the error of parents of newly build subtree to maintain the monotonicity of the error
			new_parent_error = new_parent_error * 1.1;
			update way_trees set error = new_parent_error where way_id = cur_way_id and path = parents_path[i - 1];

			-- update the node_vis_error if new error is a new maximimum for all errors the node bears
			select * into new_parent_node from current_nodes where id = (select node_id from way_trees where way_id = cur_way_id and path = parents_path[i - 1]);
			new_parent_tile = gcode_c(new_parent_node.longitude / 10000000.0, new_parent_node.latitude / 10000000.0, new_parent_error);
			update node_vis_errors set error = new_parent_error, tile = new_parent_tile where node_id = new_parent_node.id and error < new_parent_error;
		else 
			exit;
		end if;

		i = i - 1;
	end loop;

	-- update the two endpoints of the way with its path are '0' and 't' if necessary 
	if i = 1 then
		select * into lbase_tnd from way_trees where way_id = cur_way_id and path = '0';
		select * into rbase_tnd from way_trees where way_id = cur_way_id and path = 't';

		if lbase_tnd.error < new_parent_error then 
			new_parent_error = new_parent_error * 1.1;
			lbase_tnd_tile = gcode_c(lbase_nd.longitude / 10000000.0, lbase_nd.latitude / 10000000.0, new_parent_error);
			rbase_tnd_tile = gcode_c(rbase_nd.longitude / 10000000.0, rbase_nd.latitude / 10000000.0, new_parent_error);

			-- update endpoints nodes in way_trees
			update way_trees set(error, subsize) = (lbase_tnd.error, lbase_tnd.subsize + diff) where way_id = cur_way_id and path = lbase_tnd.path ;
			update way_trees set(error, subsize) = (rbase_tnd.error, rbase_tnd.subsize + diff) where way_id = cur_way_id and path = rbase_tnd.path ;

			-- update endpoints nodes in nod_vis_error
			update node_vis_errors set error = new_parent_error, tile = lbase_tnd_tile where node_id = lbase_tnd.node_id and error < new_parent_error;
			update node_vis_errors set error = new_parent_error, tile = rbase_tnd_tile where node_id = rbase_tnd.node_id and error < new_parent_error;
		else
			update way_trees set subsize = subsize + diff where way_id = cur_way_id and path = '0';
			update way_trees set subsize = subsize + diff where way_id = cur_way_id and path = 't';
		end if;
	else
		update way_trees set subsize = subsize + diff where way_id = cur_way_id and path = '0';
		update way_trees set subsize = subsize + diff where way_id = cur_way_id and path = 't';
	end if;

	return null;

END;

$$ LANGUAGE PLPGSQL;

create or replace function max_dis_from_seq(nodes_seq current_nodes[], x1 double precision, y1 double precision, x2 double precision, y2 double precision) returns real AS $$
DECLARE

	cur_dis real := 0;
	max_dis real := 0;
	nseq integer := 0;
	i integer := 1;

BEGIN
	nseq = array_length(nodes_seq, 1) ;

	while i <= nseq loop
		cur_dis = seg2pt_c(x1, y1, x2, y2, nodes_seq[i].longitude / 10000000.0, nodes_seq[i].latitude / 10000000.0);
		if cur_dis > max_dis then
			max_dis = cur_dis;
		end if;
		i = i + 1;
	end loop;
	return max_dis;
END;
$$ LANGUAGE PLPGSQL;

-- get the old nodes and replaced the target sequence with the newer onret_nds
	--TODO: change the input parameters from id to path, because of the id inconvinience and inefficiency
CREATE OR REPLACE FUNCTION fabricate_seq(cur_way_id BIGINT, elnode_path CHAR(8), ilnode_path CHAR(8), irnode_path CHAR(8), ernode_path CHAR(8), newnodes_ids BIGINT[]) RETURNS current_nodes[] as $$
DECLARE
	half1 current_nodes[];
	half2 current_nodes[];
	newnodes current_nodes[];
BEGIN

	select array_agg(ns) into half1 from current_nodes ns inner join (select node_id from way_trees where way_id = cur_way_id and path >= elnode_path and path <= ilnode_path order by path) nids on ns.id = node_id ;
	select array_agg(ns) into half2 from current_nodes ns inner join (select node_id from way_trees where way_id = cur_way_id and path >= irnode_path and path <= ernode_path order by path) nids on ns.id = node_id ;

	-- to guarantee the order we must use generate_subscript
	--select array_agg(ns) into newnodes from current_nodes ns inner join unnest(newnodes_ids) nid on ns.id = nid; 

	select array_agg(cns.* order by Y.ind) into newnodes from current_nodes cns inner join (select *, newnodes_ids[ind] as nid from (select generate_subscripts(newnodes_ids, 1) as ind) X) Y on cns.id = Y.nid ;

	-- delete the old nodes from here 
	delete from way_trees where way_id = cur_way_id and path > ilnode_path and path < irnode_path;

	return half1 || newnodes || half2;
END;
$$ LANGUAGE PLPGSQL;

-- replace node sequence with an new node sequence. 
	-- lnode and rnode are exclusively not the two points that need to be replaced. 
CREATE OR REPLACE FUNCTION seq_replace(cur_way_id BIGINT, lnode_id BIGINT, rnode_id BIGINT, new_nodes_ids BIGINT[]) RETURNS way_trees AS $$
DECLARE
	lnode_path CHAR(8);
	rnode_path CHAR(8);

	new_nodes current_nodes[];

	old_seq_num INTEGER;
	new_seq_num INTEGER;
	diff INTEGER ;

	old_seq_paths CHAR(8)[];
	lca_path CHAR(8) ;
	parents_path CHAR(8)[];
	parents_num INTEGER ;

	i INTEGER := 1;
	lsubsize INTEGER ;
	rsubsize INTEGER ;

	lend_node_path CHAR(8) := '0';
	rend_node_path CHAR(8) := 't';

	lbase_nd current_nodes;
	rbase_nd current_nodes;
	new_err	real := -1;
	new_tile CHAR(8) := '';
	x1 double precision := 0;
	y1 double precision := 0;
	x2 double precision := 0;
	y2 double precision := 0;
	parent_tnode way_trees;
	parent_node current_nodes;

	unbalanced_root_path CHAR(8) = '';
	new_seq_nodes current_nodes[];

	rebuild_path CHAR(8) := '';
	p real := 0.1;
	err real := -1.0;

	new_parent_error real := 0;
	new_parent_node current_nodes;
	new_parent_tile CHAR(8) := '';
	old_wt_error real := 0;

	lbase_tnd way_trees;
	rbase_tnd way_trees;
	lbase_tnd_tile char(8);
	rbase_tnd_tile char(8);

	lnerror real := 0;
BEGIN

	select path into lnode_path from way_trees where way_id = cur_way_id and node_id = lnode_id and path < 't';
	select path into rnode_path from way_trees where way_id = cur_way_id and node_id = rnode_id and path > '0';

	--guard corner case
	if lnode_path = '0' and rnode_path = 't' then
		new_seq_nodes = fabricate_seq(cur_way_id, '0', '0', 't', 't', new_nodes_ids);
		err = simplify(cur_way_id, new_seq_nodes, 1, array_length(new_seq_nodes, 1), p, 'T');
		return null;
	end if;

	-- two endpoints of baseline
	select * into lbase_nd from current_nodes where id = (select node_id from way_trees where way_id = cur_way_id and path = '0');
	select * into rbase_nd from current_nodes where id = (select node_id from way_trees where way_id = cur_way_id and path = 't');
	x1 = lbase_nd.longitude / 10000000.0 ; y1 = lbase_nd.latitude / 10000000.0;
	x2 = rbase_nd.longitude / 10000000.0 ; y2 = rbase_nd.latitude / 10000000.0;

	select array_agg(cns.* order by Z.ind) into new_nodes from current_nodes cns inner join (select *, new_nodes_ids[ind] as nid from (select generate_subscripts(new_nodes_ids, 1) as ind) X ) Z on Z.nid = cns.id;

	select array_agg(path) into old_seq_paths from way_trees where way_id = cur_way_id and path > lnode_path and path < rnode_path ;

	new_seq_num = array_length(new_nodes_ids, 1);
	old_seq_num = array_length(old_seq_paths, 1);
	diff = new_seq_num - old_seq_num ;

	
-- this line can also be: 
 --	lca_path = LCA(Array[lnode, rnode]);
	lca_path = LCA(old_seq_paths);
	parents_path = get_ancestors(lca_path);
	parents_path = parents_path || lca_path;

	parents_num = array_length(parents_path, 1);

	--raise info 'the parent path %', parents_path;

	while i < parents_num loop

		new_err = max_dis_from_seq(new_nodes, x1, y1, x2, y2) ;

		if parents_path[i + 1] < parents_path[i] then
			select subsize into lsubsize from way_trees where way_id = cur_way_id and path = parents_path[i + 1];
			select subsize into rsubsize from way_trees where way_id = cur_way_id and path = rchild_path(parents_path[i]);
			if lsubsize is null then lsubsize = 0 ; end if;
			if rsubsize is null then rsubsize = 0 ; end if;
			lsubsize = lsubsize + diff;

			select * into rbase_nd from current_nodes where id = (select node_id from way_trees where way_id = cur_way_id and path = parents_path[i]);
			x2 = rbase_nd.longitude / 10000000.0 ; y2 = rbase_nd.latitude / 10000000.0 ;
		else 
			select subsize into lsubsize from way_trees where way_id = cur_way_id and path = lchild_path(parents_path[i]);
			select subsize into rsubsize from way_trees where way_id = cur_way_id and path = parents_path[i + 1];
			if lsubsize is null then lsubsize = 0 ; end if;
			if rsubsize is null then rsubsize = 0 ; end if;
			rsubsize = rsubsize + diff;

			select * into lbase_nd from current_nodes where id = (select node_id from way_trees where way_id = cur_way_id and path = parents_path[i]);
			x1 = lbase_nd.longitude / 10000000.0 ; y1 = lbase_nd.latitude / 10000000.0 ;
		end if;

		if i - 1 > 0 then
			if parents_path[i] < parents_path[i - 1] then
				rend_node_path = parents_path[i - 1];
			else 
				lend_node_path = parents_path[i - 1];
			end if ;
		end if;

		if not isBalanced(lsubsize, rsubsize) then
			unbalanced_root_path = parents_path[i];
			exit;
		end if;

		-- update the node with path parents_path[i] 
		select * into parent_tnode from way_trees where way_id = cur_way_id and path = parents_path[i];
		select * into parent_node from current_nodes where id = parent_tnode.node_id;

		if new_err > parent_tnode.error then
			new_tile = gcode_c(parent_node.longitude / 10000000.0, parent_node.latitude / 10000000.0, new_err) ;
			update way_trees set (error, subsize) = (new_err, parent_tnode.subsize + diff) where way_id = cur_way_id and path = parent_tnode.path ;

			update node_vis_errors set error = new_err, tile = new_tile where node_id = parent_node.id and error < new_err;

		else
			update way_trees set (subsize) = (parent_tnode.subsize + diff) where way_id = cur_way_id and path = parent_tnode.path;
		end if;

		i = i + 1;
	end loop;

	--raise info '1: the unbalanced_root_path : % ', unbalanced_root_path;

	if unbalanced_root_path = '' then
		if lca_path < parents_path[i - 1] then
			rend_node_path = parents_path[i - 1];
		elsif lca_path > parents_path[i - 1] then
			lend_node_path = parents_path[i - 1];
		end if;
		new_seq_nodes = fabricate_seq(cur_way_id, lend_node_path, lnode_path, rnode_path, rend_node_path, new_nodes_ids);
		rebuild_path = lca_path;
		--raise info 'unbalaced_root_path empty string';
	else 
		--raise info 'unbalaced_root_path not empty string';
		new_seq_nodes = fabricate_seq(cur_way_id, lend_node_path, lnode_path, rnode_path, rend_node_path, new_nodes_ids);
		rebuild_path = unbalanced_root_path;
	end if ;

	--raise info '1: lend_node_path % ; 2: lnode_path % ; 3: rnode_path % ; 4: rend_node_path % ;' , lend_node_path, lnode_path, rnode_path, rend_node_path;

	--raise info '1: new_seq_nodes % ; 2: rebuild_path %', new_seq_nodes, rebuild_path ;
	err = simplify(cur_way_id, new_seq_nodes, 1, array_length(new_seq_nodes, 1), p, rebuild_path);

	new_parent_error = err ;

	-- update the parents' error of the newly build subtree if necessary
	while i > 1 loop
		select error into old_wt_error from way_trees where way_id = cur_way_id and path = parents_path[i - 1];

		if old_wt_error < new_parent_error then
			new_parent_error = new_parent_error * 1.1;
			update way_trees set error = new_parent_error where way_id = cur_way_id and path = parents_path[i - 1];

			select * into new_parent_node from current_nodes where id = (select node_id from way_trees where way_id = cur_way_id and path = parents_path[i - 1]);
			new_parent_tile = gcode_c(new_parent_node.longitude / 10000000.0, new_parent_node.latitude / 10000000.0, new_parent_error);

			update node_vis_errors set error = new_parent_error, tile = new_parent_tile where node_id = new_parent_node.id and error < new_parent_error;

		else
			exit;
		end if;

		i = i - 1;
	end loop;

	-- update the two endpoints of the way with its path are '0' and 't' if necessary 
	if i = 1 then
		select * into lbase_tnd from way_trees where way_id = cur_way_id and path = '0';
		select * into rbase_tnd from way_trees where way_id = cur_way_id and path = 't';

		if lbase_tnd.error < new_parent_error then 
			new_parent_error = new_parent_error * 1.1;
			lbase_tnd_tile = gcode_c(lbase_nd.longitude / 10000000.0, lbase_nd.latitude / 10000000.0, new_parent_error);
			rbase_tnd_tile = gcode_c(rbase_nd.longitude / 10000000.0, rbase_nd.latitude / 10000000.0, new_parent_error);

			-- update endpoints nodes in way_trees
			update way_trees set(error, subsize) = (lbase_tnd.error, lbase_tnd.subsize + diff) where way_id = cur_way_id and path = lbase_tnd.path ;
			update way_trees set(error, subsize) = (rbase_tnd.error, rbase_tnd.subsize + diff) where way_id = cur_way_id and path = rbase_tnd.path ;

			-- update endpoints nodes in nod_vis_error
			update node_vis_errors set error = new_parent_error, tile = lbase_tnd_tile where node_id = lbase_tnd.node_id and error < new_parent_error;
			update node_vis_errors set error = new_parent_error, tile = rbase_tnd_tile where node_id = rbase_tnd.node_id and error < new_parent_error;
		else
			-- update subsize of the 2 endpoints
			update way_trees set subsize = subsize + diff where way_id = cur_way_id and path = '0';
			update way_trees set subsize = subsize + diff where way_id = cur_way_id and path = 't';

		end if;
	
	else
		-- update subsize of the 2 endpoints
		update way_trees set subsize = subsize + diff where way_id = cur_way_id and path = '0';
		update way_trees set subsize = subsize + diff where way_id = cur_way_id and path = 't';
	end if;

	return null;

END;
$$ LANGUAGE PLPGSQL;

-- delete a node sequence from a way
	-- nodes between lnode and rnode are deleted, lnode and rnode are preserved
CREATE OR REPLACE FUNCTION seq_deletion(cur_way_id BIGINT, lnode_id BIGINT, rnode_id BIGINT) RETURNS way_trees AS $$
DECLARE
	lnode_path CHAR(8);
	rnode_path CHAR(8);

	old_seq_paths CHAR(8)[];
	old_seq_num INTEGER ;

	lca_path CHAR(8) ;
	parents_path CHAR(8)[];
	parents_num INTEGER;

	i INTEGER := 1;

	lsubsize integer := 0;
	rsubsize integer := 0;

	lend_path CHAR(8) := '0';
	rend_path CHAR(8) := 't';

	unbalanced_root CHAR(8) := '';

	parent_tnode way_trees;
	parent_node current_nodes;

	rebuild_path CHAR(8) ;
	rebuild_seq_nodes current_nodes[];
	
	p real := 0.1;
	err REAL := -1.0;

BEGIN
	select path into lnode_path from way_trees where way_id = cur_way_id and node_id = lnode_id and path < 't';
	select path into rnode_path from way_trees where way_id = cur_way_id and node_id = rnode_id and path > '0';

	--guard corner case
	if lnode_path = '0' and rnode_path = 't' then
		rebuild_seq_nodes	= fabricate_seq(cur_way_id, '0', '0', 't', 't', new_nodes_ids);
		err = simplify(cur_way_id, rebuild_seq_nodes, 1, array_length(rebuild_seq_nodes, 1), p, 'T');
		return null;
	end if;
	
	select array_agg(path) into old_seq_paths from way_trees where way_id = cur_way_id and path > lnode_path and path < rnode_path;
	old_seq_num = array_length(old_seq_paths, 1);

	lca_path = LCA(old_seq_paths);
	parents_path = get_ancestors(lca_path);
	parents_path = parents_path || lca_path;
	parents_num = array_length(parents_path, 1);

	-- seq_deletion is less compute-intensive then seq_replace and seq_insertion 
		-- because of no new nodes introduced 
	while i < parents_num loop
		if parents_path[i + 1] < parents_path[i] then
			select subsize into lsubsize from way_trees where way_id = cur_way_id and path = parents_path[i + 1];
			select subsize into rsubsize from way_trees where way_id = cur_way_id and path = rchild_path(parents_path[i]);
			if lsubsize is null then lsubsize = 0 ; end if;
			if rsubsize is null then rsubsize = 0 ; end if;
			lsubsize = lsubsize - old_seq_num;
		else 
			select subsize into lsubsize from way_trees where way_id = cur_way_id and path = lchild_path(parents_path[i]);
			select subsize into rsubsize from way_trees where way_id = cur_way_id and path = parents_path[i + 1];
			if lsubsize is null then lsubsize = 0 ; end if;
			if rsubsize is null then rsubsize = 0 ; end if;
			rsubsize = rsubsize - old_seq_num;
		end if;

		if i - 1 > 0 then
			if parents_path[i] < parents_path[i - 1] then
				rend_path = parents_path[i - 1];
			else
				lend_path = parents_path[i - 1];
			end if;

		end if;

		if not isBalanced(lsubsize, rsubsize) then
			unbalanced_root = parents_path[i];
			exit;
		end if;
		-- update the subsize for each parent node
		select * into parent_tnode from way_trees where way_id = cur_way_id and path = parents_path[i];
		-- we use the lazy update strategy to postpone the error computation
		update way_trees set (subsize) = (parent_tnode.subsize - old_seq_num) where way_id = cur_way_id and path = parents_path[i];

		i = i + 1;
	end loop;

	-- following lines survive the case in which lca_path is 'T'
	if unbalanced_root = '' then
		if lca_path < parents_path[i - 1] then
			rend_path = parents_path[i - 1];
		elsif lca_path > parents_path[i - 1] then
			lend_path = parents_path[i - 1];
		end if;

		rebuild_seq_nodes = fabricate_seq(cur_way_id, lend_path, lnode_path, rnode_path, rend_path, ARRAY[]::BIGINT[]);
		rebuild_path = lca_path;
	else
		rebuild_seq_nodes = fabricate_seq(cur_way_id, lend_path, lnode_path, rnode_path, rend_path, ARRAY[]::BIGINT[]);
		rebuild_path = unbalanced_root;
	end if;

	err = simplify(cur_way_id, rebuild_seq_nodes, 1, array_length(rebuild_seq_nodes, 1), p, rebuild_path);

	-- we choose not to re-compute the error of the parent of deleted subtrees

	-- update the endpoints subsize 

	update way_trees set subsize = subsize - old_seq_num where way_id = cur_way_id and path = '0'; 
	update way_trees set subsize = subsize - old_seq_num where way_id = cur_way_id and path = 't'; 

	return null;

END;
$$ LANGUAGE PLPGSQL;


-- function simplify

-- function get_ancestors

-- function rchild_path

-- function lchild_path

-- function isbalance3

-- function box_geohash_encode

-- function fabricate_seq

----------way_editing.sql----------------------------------------------------------------


----------query.sql----------------------------------------------------------------

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

----------query.sql----------------------------------------------------------------

----------index.sql----------------------------------------------------------------

create or replace function build_indexes() returns real as $$ 
DECLARE

BEGIN

	create index ndve_idx on node_vis_errors(tile);
	create index wt_nd_idx on way_trees(node_id);
	create index wt_wy_idx on way_trees(way_id);
	create index rt_nd_idx on relation_trees(node_id);
	create index rt_rl_idx on relation_trees(relation_id);

	return 0;
END;
$$ LANGUAGE PLPGSQL;

----------index.sql----------------------------------------------------------------

----------comp_geo.sql----------------------------------------------------------------

create or replace function seg2pt_c(double precision, double precision, double precision, double precision, double precision, double precision) returns real 
AS '$libdir/comp_geo', 'seg2pt_c'
LANGUAGE C STRICT;

create or replace function gcode_c(double precision, double precision, real) returns text
AS '$libdir/comp_geo', 'gcode_c'
LANGUAGE C STRICT;

create or replace function maxdis_c(integer[], integer[], integer, integer, integer, integer) returns real 
AS '$libdir/comp_geo', 'maxdis_c'
LANGUAGE C STRICT;

create or replace function simpl_c(bigint, real) returns real 
AS '$libdir/comp_geo', 'simpl_c'
LANGUAGE C STRICT;

create or replace function wquery_c(double precision, double precision, double precision, double precision) returns BIGINT[] 
AS '$libdir/comp_geo', 'wquery_c'
LANGUAGE C STRICT;
----------comp_geo.sql----------------------------------------------------------------

