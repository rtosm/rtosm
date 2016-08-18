
--------------------------- test case 1 --------------------------------------

create or replace function testcase1() returns integer AS $$
DECLARE

	retval INTEGER := 0;

BEGIN
	-- test seq_insertion
	retval = test_way_editing1();
	retval = test_way_editing2();
	retval = test_way_editing3();
	retval = test_way_editing_close();
	
	-- test seq_replace
	retval = test_way_rep1();
	retval = test_way_rep2();
	retval = test_way_rep3();
	retval = test_way_rep_close();
	
	-- test seq_deletion
	retval = test_way_del1();
	retval = test_way_del2();

	return 0;
END;
$$ LANGUAGE PLPGSQL;


--------------------------- test case 1 --------------------------------------

-- test case for seq_insertion
create or replace function test_way_editing1() returns integer AS $$
DECLARE
	ids BIGINT[];
	wts way_trees[];
	num_limit INTEGER := 1000;

	i INTEGER := 1;
	w_num INTEGER := 0;
	cur_way_dis real := -1;
	n_num INTEGER := 0;

	stnode way_trees;
	etnode way_trees;

	m_ind INTEGER := 0;
	mtnode way_trees;

	snode current_nodes;
	enode current_nodes;
	mnode current_nodes;

	m_dis real := -1;

	id_offset BIGINT = 2000000000;
	stile CHAR(8) := '';
	etile CHAR(8) := '';
	mtile CHAR(8) := '';

	new_way_id BIGINT = 0;
BEGIN

	select array_agg(way_id order by way_id) into ids from (select way_id from ways order by way_id limit num_limit) w_limits; 
	--select (w.*) into wts from way_trees w where way_id = any(ids) ;

	w_num = array_length(ids, 1);
	while i < w_num loop
		
		select * into stnode from way_trees where way_id = ids[i] and path = '0';
		select * into etnode from way_trees where way_id = ids[i] and path = 't';

		if etnode.subsize > 2 then 
			select * into snode from current_nodes where id = stnode.node_id;
			select * into enode from current_nodes where id = etnode.node_id; 
			m_ind = etnode.subsize / 2 + 1;
			select * into mnode from current_nodes where id = (select node_id from way_nodes where way_id = stnode.way_id and sequence_id = m_ind);

			m_dis = seg2pt_c(snode.longitude / 10000000.0, snode.latitude / 10000000.0, enode.longitude / 10000000.0, enode.latitude / 10000000.0, mnode.longitude / 10000000.0, mnode.latitude / 10000000.0);
			mtile = gcode_c(mnode.longitude / 10000000.0, mnode.latitude / 10000000.0, m_dis);

			new_way_id = ids[i] + id_offset;
			insert into way_trees (way_id, node_id, error, path, subsize) values (new_way_id, mnode.id, m_dis, 'T', 1);

			cur_way_dis = pt2pt(snode.longitude / 10000000.0, snode.latitude / 10000000.0, enode.longitude / 10000000.0, enode.latitude / 10000000.0);

			if cur_way_dis <= m_dis then
				cur_way_dis = m_dis * 1.1 ;
			end if;

			stile = gcode_c(snode.longitude / 10000000.0, snode.latitude / 10000000.0, cur_way_dis);
			etile = gcode_c(enode.longitude / 10000000.0, enode.latitude / 10000000.0, cur_way_dis);

			insert into way_trees (way_id, node_id, error, path, subsize) values (new_way_id, snode.id, cur_way_dis, '0', 3);
			insert into way_trees (way_id, node_id, error, path, subsize) values (new_way_id, enode.id, cur_way_dis, 't', 3);

		end if;
		i = i + 1;
	end loop;
	return 0;

END;

$$ LANGUAGE PLPGSQL;

--
create or replace function test_way_editing2() returns integer AS $$
DECLARE
	ids BIGINT[];
	wts way_trees[];
	num_limit INTEGER := 1000;

	i INTEGER := 1;
	w_num INTEGER := 0;
	cur_way_dis real := -1;
	n_num INTEGER := 0;

	stnode way_trees;
	etnode way_trees;

	m_ind INTEGER := 0;
	mtnode way_trees;

	snode current_nodes;
	enode current_nodes;
	mnode current_nodes;

	m_dis real := -1;

	id_offset BIGINT = 2000000000;
	stile CHAR(8) := '';
	etile CHAR(8) := '';
	mtile CHAR(8) := '';

	new_way_id BIGINT = 0;

	seq_ins1 BIGINT[];
	seq_ins2 BIGINT[];

	ret_tree way_trees;
BEGIN
	select array_agg(way_id order by way_id) into ids from (select way_id from ways order by way_id limit num_limit) w_limits; 
	--select (w.*) into wts from way_trees w where way_id = any(ids) ;

	w_num = array_length(ids, 1);
	while i < w_num loop
		
		select * into stnode from way_trees where way_id = ids[i] and path = '0';
		select * into etnode from way_trees where way_id = ids[i] and path = 't';

		if etnode.subsize > 2 then 
			select * into snode from current_nodes where id = stnode.node_id;
			select * into enode from current_nodes where id = etnode.node_id;

			m_ind = etnode.subsize / 2 + 1;
			select * into mnode from current_nodes where id = (select node_id from way_nodes where way_id = stnode.way_id and sequence_id = m_ind);

			select array_agg(cns.id order by seqs.seq) into seq_ins1 from current_nodes cns inner join (select node_id, sequence_id as seq from way_nodes where way_id = ids[i] and sequence_id > 1 and sequence_id < m_ind) seqs on cns.id = seqs.node_id ;

			if not seq_ins1 is null  then
				new_way_id = ids[i] + id_offset;
				ret_tree = seq_insertion(new_way_id, snode.id, mnode.id, seq_ins1);
			end if;

		end if;
		i = i + 1;
	end loop;
	return 0;

END;
$$ LANGUAGE PLPGSQL;


create or replace function test_way_editing3() returns integer AS $$
DECLARE
	ids BIGINT[];
	wts way_trees[];
	num_limit INTEGER := 1000;

	i INTEGER := 1;
	w_num INTEGER := 0;
	cur_way_dis real := -1;
	n_num INTEGER := 0;

	stnode way_trees;
	etnode way_trees;

	m_ind INTEGER := 0;
	mtnode way_trees;

	snode current_nodes;
	enode current_nodes;
	mnode current_nodes;

	m_dis real := -1;

	id_offset BIGINT = 2000000000;
	stile CHAR(8) := '';
	etile CHAR(8) := ''; mtile CHAR(8) := '';

	new_way_id BIGINT = 0;

	seq_ins1 BIGINT[];
	seq_ins2 BIGINT[];

	ret_tree way_trees;
BEGIN
	select array_agg(way_id order by way_id) into ids from (select way_id from ways order by way_id limit num_limit) w_limits; 
	--select (w.*) into wts from way_trees w where way_id = any(ids) ;

	w_num = array_length(ids, 1);
	while i < w_num loop
		
		select * into stnode from way_trees where way_id = ids[i] and path = '0';
		select * into etnode from way_trees where way_id = ids[i] and path = 't';

		if etnode.subsize > 2 then 
			select * into snode from current_nodes where id = stnode.node_id;
			select * into enode from current_nodes where id = etnode.node_id;

			m_ind = etnode.subsize / 2 + 1;
			select * into mnode from current_nodes where id = (select node_id from way_nodes where way_id = stnode.way_id and sequence_id = m_ind);

			select array_agg(cns.id order by seqs.seq) into seq_ins2 from current_nodes cns inner join (select node_id, sequence_id as seq from way_nodes where way_id = ids[i] and sequence_id > m_ind and sequence_id < etnode.subsize) seqs on cns.id = seqs.node_id ;

			if not seq_ins2 is null then
				new_way_id = ids[i] + id_offset;
				ret_tree = seq_insertion(new_way_id, mnode.id, enode.id, seq_ins2);
			end if;

		end if;
		i = i + 1;
	end loop;
	return 0;

END;
$$ LANGUAGE PLPGSQL;

create or replace function test_way_editing_close() returns integer AS $$
DECLARE

BEGIN
	delete from way_trees where way_id > 2000000000;
	return 0;
END;
$$ LANGUAGE PLPGSQL;

create or replace function test_way_rep1() returns integer AS $$

DECLARE
	ids BIGINT[];
	num_limit INTEGER := 1000;

	i INTEGER := 1;
	w_num INTEGER := 0;

	id_offset BIGINT = 3000000000;
BEGIN

	select array_agg(way_id order by way_id) into ids from (select way_id from ways order by way_id limit num_limit) w_limits; 
	--select array_agg(way_id order by way_id) into ids from (select way_id from ways where way_id = 4813377) w_limits;

	w_num = array_length(ids, 1);
	while i <= w_num loop
		insert into way_trees (way_id, node_id, error, path, subsize) (select ids[i] + id_offset, node_id, error, path, subsize from way_trees where way_id = ids[i]);
		i = i + 1;
	end loop;
	return 0;
end ;
$$ LANGUAGE PLPGSQL;

create or replace function test_way_rep2() returns integer AS $$

DECLARE
	ids BIGINT[];
	wts way_trees[];
	num_limit INTEGER := 1000;

	i INTEGER := 1;
	w_num INTEGER := 0;
	cur_way_dis real := -1;
	n_num INTEGER := 0;

	stnode way_trees;
	etnode way_trees;

	m_ind INTEGER := 0;
	mtnode way_trees;

	snode current_nodes;
	enode current_nodes;
	mnode current_nodes;

	m_dis real := -1;

	id_offset BIGINT = 3000000000;
	stile CHAR(8) := '';
	etile CHAR(8) := '';
	mtile CHAR(8) := '';

	new_way_id BIGINT = 0;

	seq_ins1 BIGINT[];
	seq_ins2 BIGINT[];

	ret_tree way_trees;
BEGIN
	select array_agg(way_id order by way_id) into ids from (select way_id from ways order by way_id limit num_limit) w_limits; 
	--select (w.*) into wts from way_trees w where way_id = any(ids) ;

	--select array_agg(way_id order by way_id) into ids from (select way_id from ways where way_id = 4813377) w_limits;

	w_num = array_length(ids, 1);
	while i <= w_num loop
		
		select * into stnode from way_trees where way_id = ids[i] and path = '0';
		select * into etnode from way_trees where way_id = ids[i] and path = 't';

		if etnode.subsize > 2 then 
			select * into snode from current_nodes where id = stnode.node_id;
			select * into enode from current_nodes where id = etnode.node_id;

			m_ind = etnode.subsize / 2 + 1;
			select * into mnode from current_nodes where id = (select node_id from way_nodes where way_id = ids[i] and sequence_id = m_ind);

			-- select all the nodes between the first node and the middle node
			select array_agg(cns.id order by seqs.seq) into seq_ins1 from current_nodes cns inner join (select node_id, sequence_id as seq from way_nodes where way_id = ids[i] and sequence_id > 1 and sequence_id < m_ind) seqs on cns.id = seqs.node_id ;

			--raise info '1: the snode_id : % ; 2: the mnode_id % ; 3: the nodes_ids is %', snode.id, mnode.id, seq_ins1;

			if not seq_ins1 is null  then
				new_way_id = ids[i] + id_offset;
				ret_tree = seq_replace(new_way_id, snode.id, mnode.id, seq_ins1);
			end if;

		end if; i = i + 1; end loop; return 0;
END;
$$ LANGUAGE PLPGSQL;

create or replace function test_way_rep3() returns integer AS $$

DECLARE
	ids BIGINT[];
	wts way_trees[];
	num_limit INTEGER := 1000;

	i INTEGER := 1;
	w_num INTEGER := 0;
	cur_way_dis real := -1;
	n_num INTEGER := 0;

	stnode way_trees;
	etnode way_trees;

	m_ind INTEGER := 0;
	mtnode way_trees;

	snode current_nodes;
	enode current_nodes;
	mnode current_nodes;

	m_dis real := -1;

	id_offset BIGINT = 3000000000;
	stile CHAR(8) := '';
	etile CHAR(8) := ''; mtile CHAR(8) := '';

	new_way_id BIGINT = 0;

	seq_ins1 BIGINT[];
	seq_ins2 BIGINT[];

	ret_tree way_trees;
BEGIN
	select array_agg(way_id order by way_id) into ids from (select way_id from ways order by way_id limit num_limit) w_limits; 
	--select (w.*) into wts from way_trees w where way_id = any(ids) ;

	w_num = array_length(ids, 1);
	while i < w_num loop
		
		select * into stnode from way_trees where way_id = ids[i] and path = '0';
		select * into etnode from way_trees where way_id = ids[i] and path = 't';

		if etnode.subsize > 2 then 
			select * into snode from current_nodes where id = stnode.node_id;
			select * into enode from current_nodes where id = etnode.node_id;

			m_ind = etnode.subsize / 2 + 1;
			select * into mnode from current_nodes where id = (select node_id from way_nodes where way_id = stnode.way_id and sequence_id = m_ind);

			select array_agg(cns.id order by seqs.seq) into seq_ins2 from current_nodes cns inner join (select node_id, sequence_id as seq from way_nodes where way_id = ids[i] and sequence_id > m_ind and sequence_id < etnode.subsize) seqs on cns.id = seqs.node_id ;

			if not seq_ins2 is null then
				new_way_id = ids[i] + id_offset;
				ret_tree = seq_replace(new_way_id, mnode.id, enode.id, seq_ins2);
			end if;

		end if;
		i = i + 1;
	end loop;
	return 0;

END;

$$ LANGUAGE PLPGSQL;

create or replace function test_way_rep_close() returns integer AS $$
DECLARE

BEGIN
	delete from way_trees where way_id > 3000000000;
	return 0;
END;

$$ LANGUAGE PLPGSQL;

create or replace function test_dbg() returns integer AS $$
DECLARE
	ids BIGINT[];
	num_limit INTEGER := 1000;
	w_num INTEGER := 1;
	i INTEGER := 1;

	num1 INTEGER := 0;
	num2 INTEGER := 0;
	id_offset BIGINT := 3000000000;
BEGIN
	select array_agg(way_id order by way_id) into ids from (select way_id from ways order by way_id limit num_limit) w_limits; 
	--select (w.*) into wts from way_trees w where way_id = any(ids) ;

	w_num = array_length(ids, 1);
	
	while i < w_num loop
		select count(*) into num1 from way_trees where way_id = ids[i] ;
		select count(*) into num2 from way_trees where way_id = ids[i] + id_offset ;

		if num1 <> num2 then
			raise info 'the unconsistent way is % !', ids[i] + id_offset;
		end if;

		i = i + 1;
	end loop;
	return 0;
end;
$$ LANGUAGE PLPGSQL;

-- this test shows insert or delete take effect immediatelly when 
create or replace function testVis() returns INTEGER as $$
DECLARE
	wts way_trees[];

BEGIN
	insert into way_trees (way_id, node_id , error, path, subsize, tile) values (9999999999, 2222222222, 0.7, '0', 2, 'g');
	insert into way_trees (way_id, node_id , error, path, subsize, tile) values (9999999999, 3333333333, 0.7, 't', 2, 'g');

	select array_agg(wtt.*) into wts from way_trees wtt where way_id > 9000000000;
	raise info 'the big way-tree is here : %', wts;

	delete from way_trees where way_id > 9000000000;
	select array_agg(wtt.*) into wts from way_trees wtt where way_id > 9000000000;
	raise info 'the big way-tree is here : %', wts;

	return 0;
END;
$$ LANGUAGE PLPGSQL;

create or replace function test_way_db2() returns INTEGER AS $$ 
DECLARE
	ids BIGINT[] ;
	num_limit INTEGER := 1000;

	ret1 INTEGER := 0;
	ret2 INTEGER := 0;
	ret3 INTEGER := 0;
	retc INTEGER := 0;

	id_offset BIGINT := 3000000000;
	i INTEGER := 1;
	num INTEGER := 0;

	c1 INTEGER := 0;
	c2 INTEGER := 0;

BEGIN

	ret1 = test_way_rep_close();
	ret2 = test_way_rep1();
	ret3 = test_way_rep2();
	retc = test_way_rep3();
	select array_agg(way_id order by way_id) into ids from (select way_id from ways order by way_id limit num_limit) w_limits; 
	num = array_length(ids, 1);

	while i <= num loop
		
		select count(*) into c1 from way_trees where way_id = ids[i];
		select count(*) into c2 from way_trees where way_id = ids[i] + id_offset;
		if c1 <> c2 then 

			raise info 'the inconsistent way is % : which c1 : c2 = % : %', ids[i] + id_offset, c1 , c2;

		end if;
		i = i + 1;
	end loop;
	return 0;
END;
$$ LANGUAGE PLPGSQL;

create or replace function test_way_del1() returns integer AS $$

DECLARE
	ids BIGINT[];
	num_limit INTEGER := 1000;

	i INTEGER := 1;
	w_num INTEGER := 0;

	id_offset BIGINT = 5000000000;
BEGIN

	select array_agg(way_id order by way_id) into ids from (select way_id from ways order by way_id limit num_limit) w_limits; 
	--select array_agg(way_id order by way_id) into ids from (select way_id from ways where way_id = 4813377) w_limits;

	w_num = array_length(ids, 1);
	while i <= w_num loop
		insert into way_trees (way_id, node_id, error, path, subsize) (select ids[i] + id_offset, node_id, error, path, subsize from way_trees where way_id = ids[i]);
		i = i + 1;
	end loop;
	return 0;
end ;
$$ LANGUAGE PLPGSQL;

create or replace function test_way_del2() returns integer AS $$ 

DECLARE
	ids BIGINT[];
	wts way_trees[];
	num_limit INTEGER := 1000;

	i INTEGER := 1;
	w_num INTEGER := 0;
	cur_way_dis real := -1;
	n_num INTEGER := 0;

	stnode way_trees;
	etnode way_trees;

	m_ind INTEGER := 0;
	mtnode way_trees;

	snode current_nodes;
	enode current_nodes;
	mnode current_nodes;

	m_dis real := -1;

	id_offset BIGINT = 5000000000;
	stile CHAR(8) := '';
	etile CHAR(8) := '';
	mtile CHAR(8) := '';

	new_way_id BIGINT = 0;

	seq_ins1 BIGINT[];
	seq_ins2 BIGINT[];

	ret_tree way_trees;
BEGIN
	select array_agg(way_id order by way_id) into ids from (select way_id from ways order by way_id limit num_limit) w_limits; 
	--select (w.*) into wts from way_trees w where way_id = any(ids) ;

	--select array_agg(way_id order by way_id) into ids from (select way_id from ways where way_id = 4813377) w_limits;

	w_num = array_length(ids, 1);
	while i <= w_num loop
		
		select * into stnode from way_trees where way_id = ids[i] and path = '0';
		select * into etnode from way_trees where way_id = ids[i] and path = 't';

		if etnode.subsize > 2 then 
			select * into snode from current_nodes where id = stnode.node_id;
			select * into enode from current_nodes where id = etnode.node_id;

			m_ind = etnode.subsize / 2 + 1;
			select * into mnode from current_nodes where id = (select node_id from way_nodes where way_id = ids[i] and sequence_id = m_ind);

			--raise info '1: the snode_id : % ; 2: the mnode_id % ; 3: the nodes_ids is %', snode.id, mnode.id, seq_ins1;
			-- select all the nodes between the first node and the middle node
			select array_agg(cns.id order by seqs.seq) into seq_ins1 from current_nodes cns inner join (select node_id, sequence_id as seq from way_nodes where way_id = ids[i] and sequence_id > 1 and sequence_id < m_ind) seqs on cns.id = seqs.node_id ;

			--raise info '1: the snode_id : % ; 2: the mnode_id % ; 3: the nodes_ids is %', snode.id, mnode.id, seq_ins1;

			if not seq_ins1 is null  then

				new_way_id = ids[i] + id_offset;
				ret_tree = seq_deletion(new_way_id, snode.id, mnode.id);
			--ret_tree = seq_deletion(new_way_id, mnode.id, enode.id);
			end if;

		end if;
		i = i + 1;
	end loop;

	return 0;
END;

$$ LANGUAGE PLPGSQL;
