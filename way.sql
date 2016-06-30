
--LOG
--change the relation_trees to contain the opening_member_id and closing_member_id and their types

create or replace function build_way_trees() returns real as $$ 
DECLARE
waycur SCROLL CURSOR for select distinct way_id AS C from way_nodes;
cur_way_id BIGINT;

retval real;

BEGIN
	create table if not exists way_trees(way_id BIGINT not null, 
																			node_id BIGINT not null, 
																			error REAL not null, 
																			path CHAR(8) collate "C" not null, 
																			subsize INTEGER not null, 
																			tile CHAR(8) collate "C" not null, 
																			primary key (way_id, node_id, path));
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
