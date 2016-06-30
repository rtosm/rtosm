
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
