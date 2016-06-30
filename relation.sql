-- this is a try to spped up the relation_tree build for large relations
create or replace function surb() RETURNS INTEGER AS $$
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
	create table if not exists relation_trees (relation_id BIGINT not null, 
																					 node_id BIGINT not null,
																					 opening_member_id BIGINT,
																					 opening_member_type nwr_enum,
																					 closing_member_id BIGINT,
																					 closing_member_type nwr_enum,
																					 error REAL not null,
																					 path CHAR(8) COLLATE "C" not null,
																					 subsize INTEGER not null,
																					 tile CHAR(8) COLLATE "C" not null,
																					 primary key (relation_id, node_id, path));

	ts = timeofday()::TIMESTAMP;

	--truncate table relation_trees;
	-- create a temporary table to 'cache' all the way nodes composite the relation
	--create table if not exists rwnodes (way_id BIGINT not null, sequence_id INTEGER not null, node_id BIGINT not null, lon INTEGER not null, lat INTEGER not null);

	--select array_agg(rts.relation_id order by vns.vnum DESC) into relids from relation_tags rts inner join (select * from vnumstat where vnum > 1000) vns on rts.relation_id = vns.relation_id and rts.k = 'type' and (rts.v = 'boundary' or rts.v = 'multipolygon');

	select array_agg(rts.relation_id) into relids from relation_tags rts where rts.k = 'type' and (rts.v = 'boundary' or rts.v = 'multipolygon');

	idnum = array_length(relids, 1);

	-- loop through the relations
	while i <= idnum loop
		--truncate table rwnodes;
		raise info 'the relation_tree : % begin to build', relids[i];
		select w_ids, nds into way_ids, nodes from rws(relids[i]);
		--way_ids = rws(relids[i]);
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

-- TODO: gurantee the order of the nodes by using generate_subscript for the array
	--read out nodes from relation, and its member ways
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
		-- select all the nodes from the way with way_id and insert into the rwndoes table;
		--insert into rwnodes (way_id, sequence_id, node_id, lon, lat) (select w_id, ndq.seq, ndq.ndid, ndq.lon, ndq.lat from (select cns.id as ndid, cns.longitude as lon, cns.latitude as lat, wns.sequence_id as seq from current_nodes cns inner join (select * from way_nodes where way_id = w_id) wns on cns.id = wns.node_id) ndq);

		select array_agg(swns.lon order by swns.seq), array_agg(swns.lat order by swns.seq) into sw_ndxs, sw_ndys from (select cns.longitude as lon, cns.latitude as lat, wns.sequence_id as seq from current_nodes cns inner join(select * from way_nodes where way_id = w_id) wns on cns.id = wns.node_id) swns;

	 --raise info '%', sw_ndxs;

		ndlmt = ndlmt || ndx;
		ndxs = ndxs || sw_ndxs ;
		ndys = ndys || sw_ndys ;
		ndx = ndx + array_length(sw_ndxs, 1);

	end loop;

	ndlmt = ndlmt || array_length(ndxs, 1) + 1 ;

		-- the following statement can't guarantee the order of the nodes
--  select array_agg(ns) into nds from current_nodes ns inner join unnest(nd_ids) idc on ns.id = idc;

	-- TODO: GUR ORD
	-- another sql statement to guarantee the order of the nodes
	--select array_agg(ns.* order by nar.ind) into sends from current_nodes ns inner join ( select *, nd_ids[ind] as ndid from (select generate_subscripts(nd_ids, 1) as ind) x) nar on ns.id = nar.ndid ;

	--return nds;
END;
$$ LANGUAGE PLPGSQL;

-- sort outer ring of the boundary or multipolygon
	--read out ways, pack its id into an array
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

			-- following is the debug code
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

	iscf = false;
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

--alternative function to compute the error with one stroke
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
		if cur_error is null then return -1; end if;
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

	-- update into tables
	-- TODO: does we really use the j - i - 1 as the subsize and not the actually node number which include the components way's node.
	om_id = sorted_way_ids[max_pos];
	cm_id = sorted_way_ids[max_pos - 1];

	--insert into relation_trees(relation_id, node_id, error, path, subsize, tile) values(rel_id, nds[max_pos].id, cur_error, n_path, j - i - 1, geohash_tile);
	insert into relation_trees(relation_id, node_id, opening_member_id, closing_member_id, opening_member_type, closing_member_type, error, path, subsize, tile) values (rel_id, nds[max_pos].id, om_id, cm_id, 'Way', 'Way', cur_error, n_path, j - i - 1, geohash_tile);

	--TODO: need to insert the endpoints of the relation.
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
create or replace function insert_relends(rel_id BIGINT, rel_error real, opening_way_id BIGINT, closing_way_id BIGINT, rel_nodes_num INTEGER) RETURNS INTEGER AS $$
DECLARE
	rel_snode current_nodes;
	rel_enode current_nodes;
	rel_snode_tile CHAR(8);
	rel_enode_tile CHAR(8);
BEGIN
	select * into rel_snode from current_nodes where id = (select node_id from way_nodes where way_id = opening_way_id and sequence_id = 1);
	select * into rel_enode from current_nodes where id = (select node_id from way_nodes where way_id = closing_way_id order by sequence_id DESC limit 1);

	rel_snode_tile = gcode_c(rel_snode.longitude::DOUBLE PRECISION / 10000000.0, rel_snode.latitude::DOUBLE PRECISION / 10000000.0, (rel_error * 1.1)::REAL);
	rel_enode_tile = gcode_c(rel_enode.longitude::DOUBLE PRECISION / 10000000.0, rel_enode.latitude::DOUBLE PRECISION / 10000000.0, (rel_error * 1.1)::REAL);

	insert into relation_trees(relation_id, node_id, opening_member_id, closing_member_id, opening_member_type, closing_member_type, error, path, subsize, tile) values(rel_id, rel_snode.id, opening_way_id, closing_way_id, 'Way', 'Way', rel_error * 1.1, '0', rel_nodes_num, rel_snode_tile);
	insert into relation_trees(relation_id, node_id, opening_member_id, closing_member_id, opening_member_type, closing_member_type, error, path, subsize, tile) values(rel_id, rel_enode.id, opening_way_id, closing_way_id, 'Way', 'Way', rel_error * 1.1, 't', rel_nodes_num, rel_enode_tile);
	--rect_error = error_revising(rel_id, sorted_way_ids);
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
