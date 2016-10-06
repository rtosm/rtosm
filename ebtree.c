#include "postgres.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "khash.h"
#include <utils/array.h>
#include <math.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include <catalog/pg_type.h>
#include <string.h>
#include <stdio.h>
#include "heap.h"
#include "ebtree.h"
/*
	 distance from a line segment to a point
	 the algorithm is extracted from http://geomalgorithms.com/a02-_lines.html
  */
float seg2pt(double x1, double y1, double x2, double y2, double x3, double y3) {
	double x4, y4, l2, t;
	float retval = 0;

	l2 = (x2-x1)*(x2-x1) + (y2-y1)*(y2-y1);
	if (l2 == 0) {
		retval = sqrt((x3-x1)*(x3-x1) + (y3-y1)*(y3-y1));
	} else {
		t = ((x3-x1)*(x2-x1) + (y3-y1)*(y2-y1)) / l2;
		if (t < 0) {
			retval = sqrt((x3-x1)*(x3-x1) + (y3-y1)*(y3-y1));
		} else if (t > 1) {
			retval = sqrt((x3-x2)*(x3-x2) + (y3-y2)*(y3-y2));
		} else {
			x4 = x1 + t * (x2 - x1);
			y4 = y1 + t * (y2 - y1);
			retval = sqrt((x4-x3)*(x4-x3) + (y4-y3)*(y4-y3));
		}
	}

	return retval;
}

/*
	 distance from a point to another point
 */
float pt2pt(double x1, double y1, double x2, double y2) {
	return sqrt((x1 - x2)*(x1 - x2) + (y1 - y2)*(y1 - y2));
}

/* generate the hierarchical geohash code for a give point with error 
 */
int gcode(double lon, double lat, float err, char* tile) {

	char geocode[8] ;
	char codes[] = "0123456789bcdefghjkmnpqrstuvwxyz";
	float err_levels[] = {0.54, 0.27, 0.13, 0.068, 0.034, 0.017, 0.0085, 0.0042, 0.0021, 0.001, 0.00053, 0.00026, 0.00013, 0.000065, 0.000032, 0.000016,0.0000083, 0.0000041, 0.000002, -1.0};

	short level_depth = 0;

	double lb_lat = -90;
	double lb_lon = -180;
	double rt_lat = 90;
	double rt_lon = 180;

	unsigned long geobits = 0;
	int i, mod_num, div_num, single_code;

	memset((void *)geocode, 0, 8 * sizeof(char));
	if (lon < lb_lon || lon > rt_lon || lat <  lb_lat || lat > rt_lat) {
		geocode[0] = '!';
	} else {

		for (i = 0 ; i < 20 ; i++) {
			if (err > err_levels[i]) {
				break;
			} else {
				if (lon >= (lb_lon + rt_lon) / 2.0) {
					geobits = (geobits << 1) | 0x01;
					lb_lon = (lb_lon + rt_lon) / 2.0;
				} else {
					geobits = geobits << 1;
					rt_lon = (lb_lon + rt_lon) / 2.0;
				}

				if (lat >= (lb_lat + rt_lat) / 2.0) {
					geobits = (geobits << 1) | 0x01;
					lb_lat = (lb_lat + rt_lat) / 2.0;
				} else {
					geobits = geobits << 1;
					rt_lat = (lb_lat + rt_lat) / 2.0;
				}

				level_depth++;
			}
		}

		mod_num = (level_depth * 2) % 5;
		div_num = (level_depth * 2) / 5;
		for (i = 0; i < div_num ; i++) {
			single_code = (geobits >> ((div_num - 1 - i) * 5 + mod_num)) & 0x1f;
			geocode[i] = codes[single_code];
		}
		
		//TODO here we must guard the mode_num = 0 case !
		if (mod_num > 0) {
			single_code = (geobits << (5 - mod_num)) & 0x1f;
			geocode[div_num] = codes[single_code];
		}
	}

	memcpy((void *)tile, (void *)geocode, 8 * sizeof(char));
	return 0 ;
}

/*
	 insert rows into way_trees table using data from arrays
		 insert the 2 endpoint of the way as path '0' and 't'
*/	
int arborway(long cur_way_id, long ids[], float errors[], char* paths[], int sizes[], char* tiles[], int num) {
	int i, ret_ins, ret_ins2, ret;
	char * stmnt_prefix = "insert into way_trees(way_id, node_id, error, path, subsize) values (";
	char stmnt[160];

	//char * updatetpl = "update node_vis_errors set error = %f , tile = \'%s\' where node_id = %lu and error < %f" ;
	char * updatetpl = "with upsert as (update node_err set error = %f where node_id = %ld and error < %f returning *) insert into node_err select %ld, %f where not exists (select node_id from upsert) and not exists (select node_id from node_err where node_id = %ld)" ;
	char updatestm[512];

	ret = 0;

	for (i = 0 ; i < num ; i++) {
		memset (stmnt, 0, 160);
		sprintf(stmnt, "%s %lu, %lu, %f, \'%s\', %u)", stmnt_prefix, cur_way_id, ids[i], errors[i], paths[i], sizes[i]);
		sprintf(updatestm, updatetpl, errors[i], ids[i], errors[i], ids[i], errors[i], ids[i]);

		ret_ins = SPI_exec(stmnt, 0);
		ret_ins2 = SPI_exec(updatestm, 0);
		
		if (ret_ins < 0 || ret_ins2 < 0) {
			ret = -1;
		}
	}

	return ret;
}

/*
	 algorithm of constructing the error-bound tree 
		 resulted data are stored and returns in arrays. 
	 */
float simpl(long nids[], int lons[], int lats[], int i, int j, char path[], float p, float errors[], char* paths[], int sizes[], char* tiles[]) {
	
	int k, max_pos;
	int pool = (j - i - 1) * p;
	float max_dis = -1;
	float cur_dis = -1;
	float max_pool_dis = -1;
	double x1, y1, x2, y2;
	float lerror, rerror;
	char *lpath, *rpath;
	float finalerror = -1;

	if (i + 1 >= j) {
		return 0;
	}

	x1 = lons[i] / 10000000.0;
	y1 = lats[i] / 10000000.0;
	x2 = lons[j] / 10000000.0;
	y2 = lats[j] / 10000000.0;

	max_pos = i + 1;
	for (k = i + 1; k < j ; k++) {
		cur_dis = seg2pt(x1, y1, x2, y2, lons[k] / 10000000.0, lats[k] / 10000000.0);
		if (cur_dis > max_dis) 
			max_dis = cur_dis;

		if (abs(k - (i + j) / 2 ) <= pool) {
			if (cur_dis > max_pool_dis) {
				max_pool_dis = cur_dis;
				max_pos = k;
			}
		}
	}

	lpath = palloc0(sizeof(char) * 9);
	rpath = palloc0(sizeof(char) * 9);
	lc(lpath, path);
	rc(rpath, path);
	lerror = simpl(nids, lons, lats, i, max_pos, lpath, p, errors, paths, sizes, tiles);
	rerror = simpl(nids, lons, lats, max_pos, j, rpath, p, errors, paths, sizes, tiles);

	if (lerror > rerror) 
		finalerror = lerror;
	else
		finalerror = rerror;

	if (max_dis > finalerror)
		finalerror = max_dis;
	else
		finalerror = finalerror * 1.1;

	errors[max_pos] = finalerror;
	paths[max_pos] = path;
	sizes[max_pos] = j - i - 1;
	tiles[max_pos] = palloc0(sizeof(char) * 9);
	gcode(lons[max_pos] / 10000000.0, lats[max_pos] / 10000000.0, finalerror, tiles[max_pos]);
	return finalerror;
}

/*
	 get left child path from current path
 */
int lc(char * c, char * p) {
	char lchild_codes[] = "5698=>A<EFIHMNQDUVYX]^a\\efihmnq";
	int plen = strlen(p);
	char tailch = p[plen - 1];
	int chcode = (int) tailch;
	memcpy((void *)c, (void *)p, (plen - 1) * sizeof(char));
	c[plen - 1] = lchild_codes[(chcode - 52) / 2 - 1];
	if ((int)(c[plen - 1]) % 2 == 1) c[plen] = 'T';
	c[9] = 0;
	return 0;
}

/*
	 get right child path from current path
 */
int rc(char * c, char * p) {
	char rchild_codes[] = "7:;@?BCLGJKPORSdWZ[`_bclgjkpors";
	int plen = strlen(p);
	char tailch = p[plen - 1];
	int chcode = (int) tailch;
	memcpy((void *)c, (void *)p, (plen - 1) * sizeof(char));
	c[plen - 1] = rchild_codes[(chcode - 52) / 2 - 1];
	if ((int)(c[plen - 1]) % 2 == 1) c[plen] = 'T';
	c[9] = 0;
	return 0;
}

/* get ancestors of way_trees or relation_trees and store it into a hashset */
	//TODO: caution with the memory managment
int get_ancestors(char * npath, khash_t(path) * hpath) {
	char ancestor_table[63][5] = {"68<DT","8<DTz", "68<DT", "<DTzz", ":8<DT", "8<DTz",  ":8<DT", "DTzzz", ">@<DT", "@<DTz", ">@<DT", "<DTzz", "B@<DT", "@<DTz", "B@<DT", "Tzzzz", "FHLDT", "HLDTz", "FHLDT", "LDTzz", "JHLDT", "HLDTz", "JHLDT", "DTzzz", "NPLDT", "PLDTz", "NPLDT", "LDTzz", "RPLDT", "PLDTz", "RPLDT", "zzzzz", "VX\\dT", "X\\dTz", "VX\\dT", "\\dTzz", "ZX\\dT", "X\\dTz", "ZX\\dT", "dTzzz", "^`\\dT", "`\\dTz", "^`\\dT", "\\dTzz", "b`\\dT", "`\\dTz", "b`\\dT", "Tzzzz", "fhldT", "hldTz", "fhldT", "ldTzz", "jhldT", "hldTz", "jhldT", "dTzzz", "npldT", "pldTz", "npldT", "ldTzz", "rpldT", "pldTz", "rpldT"} ;
	char ch ;
	int i = 0, len = 0, j = 0, ind = 0, abs = 0, cnt = 0 ;
	char * prefix = palloc0(9 * sizeof(char));
	char * tmp;

	/* initialization of variables */
	len = strlen(npath);
	for (i = 0 ; i < len ; i++) {
		ch = npath[i];
		ind = ch - 53;
		if (ind < 0 || ind >= 63) return 0;
		for (j = 4 ; j >= 0 ; j--) {
			prefix[i] = ancestor_table[ind][j];
			prefix[i + 1] = 0;
			if (prefix[i] != 'z' ) {
				kh_put(path, hpath, prefix, &abs);
				if (abs) {
					tmp = prefix;
					prefix = palloc0(9 * sizeof(char));
					strcpy(prefix, tmp);
				}
				cnt += 1;
			} 
		}
		prefix[i] = ch;
		if (ind % 2 > 0) break;
	}
	return cnt;
}

/* calculate the tiles from window   */
int window2tiles(double wx1, double wy1, double wx2, double wy2, khash_t(str) * h, char * tiles[]) {
	double xspn, yspn, spn, tole;
	int i, abs;

	/* get the major span of the window */
	xspn = wx2 - wx1 ;
	yspn = wy2 - wy1 ;

	if (xspn > yspn) {
		spn = xspn ;
	} else {
		spn = yspn ;
	}

	tole = spn / 2048.0 * 3.0;
	for (i = 0 ; i < 80 ; i++) {
		tiles[i] = palloc0(9 * sizeof(char));
	}

	i = 0;
	// 0.55 is a magic number that is slightly above 360 / 2000 * 3.0 = 0.54, that mean the largest error level in a 
		// top tile 
	while (tole < 0.55) {
		gcode(wx1, wy1, tole, tiles[i]);
		gcode(wx1, wy2, tole, tiles[i + 1]);
		gcode(wx2, wy2, tole, tiles[i + 2]);
		gcode(wx2, wy1, tole, tiles[i + 3]);

		kh_put(str, h, tiles[i], &abs);
		kh_put(str, h, tiles[i + 1], &abs);
		kh_put(str, h, tiles[i + 2], &abs);
		kh_put(str, h, tiles[i + 3], &abs);
		
		tole = tole * 2;
		i = i + 4;
	}
	
	return i;
}

ArrayType * iinids2array(khash_t(nid) * hnid, long *nids, int nidlen) {
	ArrayType * retarr ;
	int hnidlen = 0, count, i, abs;
	Datum * vals ;

	for (i = 0 ; i < nidlen ; ++i) {
		//vals[count++] = Int64GetDatum(nids[i]);
		kh_put(nid, hnid, nids[i], &abs);
	}

	hnidlen = kh_size(hnid);
	vals = palloc0(sizeof(Datum) * hnidlen);
	count = 0;

	for (i = kh_begin(hnid) ; i != kh_end(hnid) ; ++i) {
		if (kh_exist(hnid, i)) vals[count++] = Int64GetDatum(kh_key(hnid, i));
	}


	retarr = construct_array(vals, hnidlen + nidlen, INT8OID, sizeof(long), true, 'i');

	return retarr;
}

// tile must be zeroed
int gcode2(long xy_bits, int bits_count, char* tile) {
	char codes[] = "0123456789bcdefghjkmnpqrstuvwxyz";
	int quotient = (bits_count * 2) / 5;
	int remainder = (bits_count * 2) % 5;
	long code_n = 0;
	int ch_ind = 0, i;

	for (i = quotient - 1 ; i >= 0 ; i--) {
		code_n = (xy_bits >> (i * 5 + remainder)) & 0x000000000000001full ;
		tile[ch_ind++] = codes[code_n];
	}

	//for the remainder
	if (remainder > 0) {
		code_n = (xy_bits << (5 - remainder)) & 0x000000000000001full ;
		tile[ch_ind++] = codes[code_n];
	}

	return bits_count;
}

int dump2file(qtree * qn, FILE * fdd, int level) {
	fprintf(fdd, "Mem: %p ; count: %d ; level: %d ; tile : %s \n", qn, qn->count, level, qn->tile);
	if (qn->children[0]) {
		dump2file(qn->children[0], fdd, level + 1);
		dump2file(qn->children[1], fdd, level + 1);
		dump2file(qn->children[2], fdd, level + 1);
		dump2file(qn->children[3], fdd, level + 1);
	}
	return 0;
}

int compare_tt_keys(void *key1, void *key2) {
	float kf1 = *((float *)key1);
	float kf2 = *((float *)key2);

	if (kf1 < kf2) {
		return 1;
	} else if (kf1 == kf2) {
		return 0;
	} else {
		return -1;
	}
}

//read tile from database 
Tile_tree* read_tile(char* tile, int level, Tile_tree * ttp) {

	int ret, proc;
	char tile_query[256];
	char * tile_str;

	sprintf(tile_query, "select tile_level, tile, nums, error_ubound, error_lbound from tile_nums where tile_level = %d and tile = \'%s\'", level, tile);

	ret = SPI_execute(tile_query, true, 0);
	proc = SPI_processed;

	if (ret > 0 && SPI_tuptable != NULL) {
		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		SPITupleTable * tuptable = SPI_tuptable;

		if (proc > 0) {
			HeapTuple tuple = tuptable->vals[0];

			ttp->level	= atoi(SPI_getvalue(tuple, tupdesc, 1)) ;
			tile_str		= SPI_getvalue(tuple, tupdesc, 2) ;
			ttp->count	= atoi(SPI_getvalue(tuple, tupdesc, 3)) ;
			ttp->eub		= atof(SPI_getvalue(tuple, tupdesc, 4)) ;
			ttp->elb		= atof(SPI_getvalue(tuple, tupdesc, 5)) ;
			strcpy(ttp->tile, tile_str);

		} else {
			//there is no entry in tile_nums with the specified level and tile
		}
	} else {
			//there is no entry in tile_nums with the specified level and tile
	}
	
	return ttp;
}

int covered_nodes_number(Tile_tree * ttp, double wx1, double wy1, double wx2, double wy2, bool *isOverlapped) {
	double xlb = -180, xub = 180, ylb = -90, yub = 90, xspan = 0 , yspan = 0, overlaparea, tilearea;
	int retval = 0, xbit = 0, ybit = 0, i ;

	for (i = 0 ; i < ttp->level ; i++) {
		xbit = (ttp->geobits >> ((ttp->level - 1 - i) * 2 + 1)) & 0x1;
		ybit = (ttp->geobits >> (ttp->level - 1 - i) * 2) & 0x1 ;

		if (xbit) {
			xlb = (xlb + xub) / 2 ;
		} else {
			xub = (xlb + xub) / 2 ;
		}

		if (ybit) {
			ylb = (ylb + yub) / 2 ;
		} else {
			yub = (ylb + yub) / 2 ;
		}
	}

	//test if the target area overlapped with the tile
	if (wx1 >= xub || wx2 <= xlb || wy1 >= yub || wx2 <= ylb) {
		*isOverlapped = false;
		return retval ;
	} else {
		//compute the xspan and yspan seprately 
			// x first
		if (wx1 > xlb && wx1 < xub && wx2 > xlb && wx2 < xub) {
			xspan = wx2 - wx1;
		} else if (xlb > wx1 && xlb < wx2 && xub > wx1 && xub < wx2) {
			xspan = xub - xlb;
		} else {
			xspan = (xub - wx1) < (wx2 - xlb) ? (xub - wx1) : (wx2 - xlb);
		}

		if (wy1 > ylb && wy1 < yub && wy2 > ylb && wy2 < yub) {
			yspan = wy2 - wy1;
		} else if (ylb > wy1 && ylb < wy2 && yub > wy1 && yub < wy2) {
			yspan = yub - ylb;
		} else {
			yspan = (yub - wy1) < (wy2 - ylb) ? (yub - wy1) : (wy2 - ylb);
		}

		overlaparea = xspan * yspan;
		tilearea = (xub - xlb) * (yub - ylb);
		retval = (overlaparea / tilearea) * ttp->count;
		if (retval > 0) *isOverlapped = true;
		return retval;
	}
}

//calculate the error of a specific number of nodes
heap* dig_down(heap* h, Tile_tree * ttp) {
	
	unsigned long geobits = 0;
	Tile_tree * tile_children[4] ;
	int i;

		// if target area contains the tile_tree node
	geobits = tile2gbits(ttp->tile, ttp->level);

	for (i = 0 ; i < 4 ; i++) {
		tile_children[i] = palloc0(sizeof(Tile_tree));
		gbits2tile((geobits << 2) | i, ttp->level + 1,  tile_children[i]->tile);
		read_tile(tile_children[i]->tile, ttp->level + 1, tile_children[i]);
		heap_infix(h, (void *)&(tile_children[i]->elb), (void *)tile_children[i]);
	}
	return h;
} 

unsigned long tile2gbits(char * tile, int level) {
	unsigned long retval = 0;

	int magic = 48, i;
	int revcode[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1, -1, -1,
	 						  	10, 11, 12, 13, 14, 15, 16, -1, 17, 18, -1, 19, 20, -1, 21, 22,
								 	23, 24, 25, 26, 27, 28, 29, 30, 31};

	int len = strlen(tile);
	for (i = 0 ; i < len ; i++) {
		if (tile[i] >= 97) {
			retval = (retval << 5) | (revcode[((int)tile[i] - magic) - 32]);
		} else {
			retval = (retval << 5) | (revcode[(int)tile[i] - magic]) ;
		}
	}
	return retval;
}

char* gbits2tile(unsigned long geobits, int level_depth, char geocode[]) {
	//char * geocode = palloc0(9 * sizeof(char));
	char codes[] = "0123456789bcdefghjkmnpqrstuvwxyz";
	int mod_num = (level_depth * 2) % 5;
	int div_num = (level_depth * 2) / 5;
	int i;
	for (i = 0; i < div_num ; i++) {
		int single_code = (geobits >> ((div_num - 1 - i) * 5 + mod_num)) & 0x1f;
		geocode[i] = codes[single_code];
	}
	
	//TODO here we must guard the mode_num = 0 case !
	if (mod_num > 0) {
		int single_code = (geobits << (5 - mod_num)) & 0x1f;
		geocode[div_num] = codes[single_code];
	}
	return geocode;
}
