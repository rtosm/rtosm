
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

	-- Do we need to insert the node into way_tree when it complete the computing of err, path, subsize and tile?
		-- if exist then update (in the case of ways editing, seq_replace, some node need to be udpated to ), insert otherwise
	select * into existing_node from way_trees where node_id = subline[pool_max_pos].id and way_id = cur_way_id ; -- and path = subline[pool_max_pos].path ;
	if FOUND then
		update way_trees set (error, path, subsize, tile) = (finalerror, pth, j - i - 1, gtl) where node_id = subline[pool_max_pos].id and way_id = cur_way_id  ; --and path = subline[pool_max_pos].path;
	else 
		insert into way_trees(way_id, node_id, error, path, subsize, tile) values (cur_way_id, subline[pool_max_pos].id, finalerror, pth, j - i - 1, gtl);
	end if;
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
