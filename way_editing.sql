
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

	while i < parents_num loop
	-- from i to parents_num - 1, compute the error from the ith parents to the inserted node sequence.
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
			x1 = lbase_nd.longitude / 10000000.0 ; y2 = lbase_nd.latitude / 10000000.0;
		end if;

		if i - 1 > 0 then
			if parents_path[i] < parents_path[i - 1] then
				rend_path = parents_path[i - 1];
			else 
				lend_path = parents_path[i - 1];
			end if;
		end if;

		raise info 'lsubsize is % ; rsubsize is %', lsubsize, rsubsize;
		if not isBalanced(lsubsize, rsubsize) then 
			unbalanced_root = parents_path[i];
			exit;
		end if;

		select * into parent_tnode from way_trees where way_id = cur_way_id and path = parents_path[i];
		select * into parent_node from current_nodes where id = parent_tnode.node_id;

		new_err = max_dis_from_seq(new_nodes, x1, y1, x2, y2);
		-- update the parent_tnode's error(possible) subsize, and tile(possible) ;
		if new_err > parent_tnode.error then
			new_tile = gcode_c(parent_node.longitude / 10000000.0, parent_node.latitude / 10000000.0, new_err);
			update way_trees set (error, subsize, tile) = (new_err, parent_tnode.subsize + diff, new_tile) where way_id = cur_way_id and path = parents_path[i];
		else
			update way_trees set (subsize) = (parent_tnode.subsize + diff) where way_id = cur_way_id and path = parents_path[i];
		end if;

		i = i + 1;
	end loop;

	raise info 'unbalanced_root_path is %', unbalanced_root;
	-- wether or not the unbalanced_root is not found
	if unbalanced_root = '' then
		subtree_nodes = fabricate_seq(cur_way_id, lnode.path, lnode.path, rnode.path, rnode.path, new_nodes_ids);
		rebuild_path = new_seq_path;
	else 
		subtree_nodes = fabricate_seq(cur_way_id, lend_path, lnode.path, rnode.path, rend_path, new_nodes_ids);
		rebuild_path = unbalanced_root;
	end if;

	-- using simplify
	err = simplify(cur_way_id, subtree_nodes, 1, array_length(subtree_nodes, 1), p , rebuild_path);

	-- every parents should update its error
		-- parents to calculate the distance from its

	-- update the '0' and 't' node with different error, subsize and tile.
	select * into lbase_tnd from way_trees where way_id = cur_way_id and path = '0';
	select * into rbase_tnd from way_trees where way_id = cur_way_id and path = 't';

	if lbase_tnd.error < err then  
		lbase_tnd.error = err * 1.1 ;
	end if;
	if rbase_tnd.error < err then
		rbase_tnd.error = err * 1.1 ;
	end if;

	lbase_tnd_tile = gcode_c(lbase_nd.longitude / 10000000.0, lbase_nd.latitude / 10000000.0, lbase_tnd.error);
	rbase_tnd_tile = gcode_c(rbase_nd.longitude / 10000000.0, rbase_nd.latitude / 10000000.0, rbase_tnd.error);

	update way_trees set(error, subsize, tile) = (lbase_tnd.error, lbase_tnd.subsize + diff, lbase_tnd_tile) where way_id = cur_way_id and path = lbase_tnd.path ;
	update way_trees set(error, subsize, tile) = (rbase_tnd.error, rbase_tnd.subsize + diff, rbase_tnd_tile) where way_id = cur_way_id and path = rbase_tnd.path ;

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

		select * into parent_tnode from way_trees where way_id = cur_way_id and path = parents_path[i];
		select * into parent_node from current_nodes where id = parent_tnode.node_id;

		if new_err > parent_tnode.error then
			new_tile = gcode_c(parent_node.longitude / 10000000.0, parent_node.latitude / 10000000.0, new_err) ;
			update way_trees set (error, subsize, tile) = (new_err, parent_tnode.subsize + diff, new_tile) where way_id = cur_way_id and path = parent_tnode.path ;
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
	return null;

END;
$$ LANGUAGE PLPGSQL;

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
	
	select array_agg(path) into old_seq_paths from way_trees where way_id = cur_way_id and path > lnode_path and path < rnode_path;
	old_seq_num = array_length(old_seq_paths, 1);

	lca_path = LCA(old_seq_paths);
	parents_path = get_ancestors(lca_path);
	parents_path = parents_path || lca_path;
	parents_num = array_length(parents_path, 1);

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
