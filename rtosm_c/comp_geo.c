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

PG_MODULE_MAGIC;

/* external functions to be called by SQL statements */
Datum seg2pt_c(PG_FUNCTION_ARGS) ;
Datum gcode_c(PG_FUNCTION_ARGS) ;
Datum maxdis_c(PG_FUNCTION_ARGS) ;
Datum simpl_c(PG_FUNCTION_ARGS) ;
Datum wquery_c(PG_FUNCTION_ARGS) ;

/* internal functions that only called by functions in this file */
float seg2pt(double x1, double y1, double x2, double y2, double x3, double y3) ;
float pt2pt(double x1, double y1, double x2, double y2) ;
int gcode(double lon, double lat, float err, char* tile) ;
int lc(char * c, char * p) ;
int rc(char * c, char * p) ;
float simpl(long nids[], int lons[], int lats[], int i, int j, char path[], float p, float errors[], char* paths[], int sizes[], char* tiles[]) ;
int arborway(long cur_way_id, long ids[], float errors[], char* paths[], int sizes[], char* tiles[], int num) ;

/* Initialization of the khash related functions */
KHASH_SET_INIT_STR(str)
KHASH_SET_INIT_STR(path)
KHASH_SET_INIT_INT64(nid)

int get_ancestors(char * npath, khash_t(path) * hpath) ;
int window2tiles(double wx1, double wy1, double wx2, double wy2, khash_t(str) * h, char *ptiles[]) ;
ArrayType * iinids2array(khash_t(nid) * hnid, long *nids, int nidlen) ;

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

PG_FUNCTION_INFO_V1(seg2pt_c);

/* distance from a line segment to a point
	 a wrapper function for PL/pgsql 

*/

Datum seg2pt_c(PG_FUNCTION_ARGS) {
	float8 x1,y1,x2,y2,x3,y3;
	float4 retval = 0;

	x1 = PG_GETARG_FLOAT8(0);
	y1 = PG_GETARG_FLOAT8(1);
	x2 = PG_GETARG_FLOAT8(2);
	y2 = PG_GETARG_FLOAT8(3);
	x3 = PG_GETARG_FLOAT8(4);
	y3 = PG_GETARG_FLOAT8(5);

	retval = seg2pt(x1, y1, x2, y2, x3, y3);

	PG_RETURN_FLOAT4(retval);
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

	long geobits = 0;
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
		
		single_code = (geobits << (5 - mod_num)) & 0x1f;
		geocode[div_num] = codes[single_code];
	}

	memcpy((void *)tile, (void *)geocode, 8 * sizeof(char));
	return 0 ;
}

PG_FUNCTION_INFO_V1(gcode_c);

/* wrapper function for PL/pgsql
 */
Datum gcode_c(PG_FUNCTION_ARGS) {
	text * retval ;
	float8 lon, lat;
	float4 err;

	char geocode[8] ;

	lon = PG_GETARG_FLOAT8(0);
	lat = PG_GETARG_FLOAT8(1);
	err = PG_GETARG_FLOAT4(2);

	memset((void *)geocode, 0, 8 * sizeof(char));
	gcode(lon, lat, err, geocode);

	retval = (text *) palloc0(8 * sizeof(char) + VARHDRSZ); 
	SET_VARSIZE(retval, 8 + VARHDRSZ);
	memcpy((void *)VARDATA(retval), (void *)geocode, 8 * sizeof (char));
	PG_RETURN_TEXT_P(retval);
}

PG_FUNCTION_INFO_V1(maxdis_c);

/*
	 get the maximum distance from an array of nodes to a base line
		 using in the relation_trees building
 */ 

Datum maxdis_c(PG_FUNCTION_ARGS) {

	ArrayType * xinput, *yinput;
	Datum * xs, *ys;
	int32 x1, y1, x2, y2, x3, y3;

	Oid i_eltype;
	int16 i_typlen;
	bool i_typbyval;
	char i_typalign;
	bool *nulls;

	float curdis = -1;
	float maxdis = -1;
	int i, n;

	xinput = PG_GETARG_ARRAYTYPE_P(0);
	yinput = PG_GETARG_ARRAYTYPE_P(1);

	x1 = PG_GETARG_INT32(2);
	y1 = PG_GETARG_INT32(3);
	x2 = PG_GETARG_INT32(4);
	y2 = PG_GETARG_INT32(5);

	i_eltype = ARR_ELEMTYPE(xinput);

	get_typlenbyvalalign(i_eltype, &i_typlen, &i_typbyval, &i_typalign);

	deconstruct_array(xinput, i_eltype, i_typlen, i_typbyval, i_typalign, &xs, &nulls, &n);
	deconstruct_array(yinput, i_eltype, i_typlen, i_typbyval, i_typalign, &ys, &nulls, &n);

	for (i = 0 ; i < n ; i++) {
		x3 = DatumGetInt32(xs[i]);
		y3 = DatumGetInt32(ys[i]);

		curdis = seg2pt(x1/10000000.0,y1/10000000.0,x2/10000000.0,y2/10000000.0,x3/10000000.0,y3/10000000.0);
		if (maxdis < curdis) {
			maxdis = curdis;
		}
	}
	PG_RETURN_FLOAT4(maxdis);
}

PG_FUNCTION_INFO_V1(simpl_c);
/*
	 build a way_tree from a way id
	 	will insert rows into way_trees table
	 */
Datum simpl_c(PG_FUNCTION_ARGS) {
	char * prepst = "select ns.id, ns.longitude, ns.latitude from current_nodes ns inner join (select node_id as nid, sequence_id as seq from way_nodes where way_id = %lu) nids on ns.id = nids.nid order by nids.seq";
	char * stmnt = palloc0(sizeof(char) * (strlen(prepst) + 32));

	int64 wid;
	long * ids = 0;

	int ret, proc, * lons, *lats;
	float  p, root_err;
	// construct the array of errors, path, size and tile
	float * errors;
	char ** paths;
	int * sizes;
	char **tiles;
	int num;
	//char * abc;

	float way_err ;
	char * sntile, * entile;

	wid = PG_GETARG_INT64(0);
	p		= PG_GETARG_FLOAT4(1);
	root_err = 0;

	// prepare the query statement
	sprintf(stmnt, prepst, wid);

	SPI_connect();

	ret = SPI_exec(stmnt, 0);

	proc = SPI_processed;

	if (ret > 0 && SPI_tuptable != NULL) {
		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		SPITupleTable *tuptable = SPI_tuptable;
		int i ;

		ids 	= palloc(sizeof(long) * proc);
		lons 	= palloc(sizeof(int) * proc);
		lats 	= palloc(sizeof(int) * proc);

		for (i = 0; i < proc ; i++ ) {
			HeapTuple tuple = tuptable->vals[i];

			ids[i]	= atol(SPI_getvalue(tuple, tupdesc, 1)) ;
			lons[i] = atoi(SPI_getvalue(tuple, tupdesc, 2)) ;
			lats[i] = atoi(SPI_getvalue(tuple, tupdesc, 3)) ;
		}

		errors = palloc(sizeof (float) * proc);
		paths = palloc(sizeof (char*) * proc);
		sizes = palloc(sizeof (int) * proc);
		tiles = palloc(sizeof (char*) * proc);

		//-------------------------------------------------------------
		root_err = simpl(ids, lons, lats, 0, proc - 1, "T", p, errors, paths, sizes, tiles);
		way_err = pt2pt(lons[0] / 10000000.0, lats[0] / 10000000.0, lons[proc - 1] / 10000000.0, lats[proc - 1] / 10000000.0);
		num = proc;

		sntile = palloc0(sizeof(char) * 9);
		entile = palloc0(sizeof(char) * 9);
		if (way_err <= root_err)	way_err = root_err * 1.1;
		gcode(lons[0] / 10000000.0, lats[0] / 10000000.0, way_err, sntile);
		gcode(lons[proc - 1] / 10000000.0, lats[proc - 1] / 10000000.0, way_err, entile);
		
		errors[0] = way_err;
		errors[proc - 1] = way_err;
		paths[0] = "0";
		paths[proc - 1] = "t";
		sizes[0] = proc ;
		sizes[proc - 1] = proc;
		tiles[0] = sntile;
		tiles[proc - 1] = entile;

		/*
		abc 	= palloc0(sizeof(char) * 300);
		memset((void *) abc, 0, sizeof(char) * 300);
		sprintf(abc, "insert into ns(dstring) values ( \'node_id : %lu ; lon : %d ; lat : %d; tile : %s \')", ids[0], lons[0], lats[0], tiles[0]);
		SPI_exec(abc, 0);
		memset((void *) abc, 0, sizeof(char) * 300);
		sprintf(abc, "insert into ns(dstring) values ( \'node_id : %lu ; lon : %d ; lat : %d; tile : %s \')", ids[proc - 1], lons[proc - 1], lats[proc - 1], tiles[proc - 1]);
		SPI_exec(abc, 0);
		*/

		//-------------------------------------------------------------
		//insert wid, nid, error, path, subsize, tile into table way_trees;
		arborway(wid, ids, errors, paths, sizes, tiles, num);

		// free all the resources we have allocated
		pfree(ids);
		pfree(lons);
		pfree(lats);
		pfree(errors);
		pfree(paths);
		pfree(sizes);
		pfree(tiles);

		//finished the connect to the SPI
		SPI_finish();

		//free the resource;
		pfree(stmnt);
	}

	PG_RETURN_FLOAT4(root_err);
}

/*
	 insert rows into way_trees table using data from arrays
		 insert the 2 endpoint of the way as path '0' and 't'
*/	
int arborway(long cur_way_id, long ids[], float errors[], char* paths[], int sizes[], char* tiles[], int num) {
	int i, ret_ins, ret_ins2, ret;
	char * stmnt_prefix = "insert into way_trees(way_id, node_id, error, path, subsize) values (";
	char stmnt[160];

	char * updatetpl = "update node_vis_errors set error = %f , tile = \'%s\' where node_id = %lu and error < %f" ;
	char updatestm[160];


	ret = 0;

	for (i = 0 ; i < num ; i++) {
		memset (stmnt, 0, 160);
		sprintf(stmnt, "%s %lu, %lu, %f, \'%s\', %u)", stmnt_prefix, cur_way_id, ids[i], errors[i], paths[i], sizes[i]);
		sprintf(updatestm, updatetpl, errors[i], tiles[i], ids[i], errors[i]);

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

PG_FUNCTION_INFO_V1(wquery_c);
/*
	the parameters is the coordinates of the leftbottom and topright point of the querying window

*/
Datum wquery_c(PG_FUNCTION_ARGS) {
	double wx1, wy1, wx2, wy2;
	khash_t(str) * h;
	khash_t(path) * hpath;
	khash_t(nid) * hnid;
	int i, j, ret, proc, ind, abs, count, hnidlen;
	unsigned wx1uint, wy1uint, wx2uint, wy2uint;

	char * pre_stmt = "select way_id, node_id, path from way_trees where node_id in (select cn.id from (select node_id from node_vis_errors nve where tile in (%s)) nve inner join current_nodes cn on cn.id = nve.node_id and cn.longitude >= %d and cn.longitude <= %d and cn.latitude >= %d and cn.latitude <= %d) order by way_id, path;";
	char stmnt[1500] ;
	char tilestr[1024];

	long * wids, * nids;
	char ** paths, * tiles[80];
	ArrayType * retarr;
	Datum * vals ;

	/* for debug purpose */
	FILE * fdd = fopen("/tmp/wquery.txt", "a");
	char ttstr[16];

	/* get the coordinates of the window  */
	wx1 = PG_GETARG_FLOAT8(0);
	wy1 = PG_GETARG_FLOAT8(1);
	wx2 = PG_GETARG_FLOAT8(2);
	wy2 = PG_GETARG_FLOAT8(3);

	/* initialization of 3 hash sets
		1. for tiles conditions
		2. for paths of ancestors of a way's directly included nodes
		3. for id of indirectly included nodes
		*/
	h = kh_init(str);
	hpath = kh_init(path);
	hnid = kh_init(nid);

	/* from window to tiles conditions  */
	window2tiles(wx1, wy1, wx2, wy2, h, tiles);

	/* from tiles condition to directly included nodes, ways and relations */
	/* tiles2dinids(wx1, wy1, wx2, wy2, h, &wids, &nids, &paths);  */

	/* from directly included nodes into indirectly included nodes using way_trees and relation_trees */
	/* dinids2iinids(h, wids, nids, paths, hpath, hnid); */
		
	wx1uint = wx1 * 10000000;
	wy1uint = wy1 * 10000000;
	wx2uint = wx2 * 10000000;
	wy2uint = wy2 * 10000000;

	/* Initialize char string for database querying */
	memset((void *)stmnt, 0, 1500 * sizeof(char));
	memset((void *)tilestr, 0, 1024 * sizeof(char));

	ind = 0;
	for (i = kh_begin(h) ; i != kh_end(h); ++i) {
		if (kh_exist(h, i)) {
			if (0 == ind) {
				ind = sprintf (tilestr, "\'%s\'", kh_key(h, i));
			} else {
				ind += sprintf (&tilestr[ind], ",\'%s\'", kh_key(h, i));
			}
		}
	}
	
	sprintf(stmnt, pre_stmt, tilestr, wx1uint, wx2uint, wy1uint, wy2uint);
	/* begin to query the database to retrieve the data */
	SPI_connect();

	/* for debug purpose */
	if (fdd != NULL) {
		fputs (stmnt, fdd);
		fputs ("\n", fdd);
	}

	ret = SPI_execute(stmnt, true, 0);

	proc = SPI_processed;

	/* for an empty results */
	if (0 == proc) {
		SPI_finish();
		PG_RETURN_ARRAYTYPE_P(construct_empty_array(INT8OID));
	}

	/* for debug purpose */
	if (fdd != NULL) {
		sprintf(ttstr, "Proc = %u\n",proc);
		fputs (ttstr, fdd);
	}

	/* allocate the memory for the results */
	wids = palloc0((proc + 1) * sizeof(long));
	nids = palloc0((proc + 1) * sizeof(long));
	paths = palloc0((proc + 1) * sizeof(char *));

	/* read out the actual data */
	if (ret > 0 && SPI_tuptable != NULL) {
		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		SPITupleTable * tuptable = SPI_tuptable;
		long wid = 0, pre_wid = 0, nid;
		char *npath, * pathstr = 0 ;
		int path_con_len = 0, ret2, proc2 ;
		char * path2id_tpl = "select node_id from way_trees where way_id = %lu and path in (%s)"; 
		char * path2id = 0;
		char ttstr2[80];

		/* copy the data retrieved from table into arrays  */
		for (i = 0 ; i < proc; i++) {
			HeapTuple tuple = tuptable->vals[i];
			
			wid = atol(SPI_getvalue(tuple, tupdesc, 1));
			nid = atol(SPI_getvalue(tuple, tupdesc, 2));
			npath = SPI_getvalue(tuple, tupdesc, 3);

			wids[i] = wid;
			nids[i] = nid;
			paths[i] = npath;
		}

		/* sentinel for the last actual way to be processed */
		wids[proc] = 0;
		nids[proc] = nids[0];
		paths[proc] = paths[0];

		/* extend each way's nodes from directly included nodes to indirectly included nodes */
		for (i = 0 ; i <= proc; i++) {
			
			wid = wids[i];
			nid = nids[i];
			npath = paths[i];

			/* for debug purpose */
			if (fdd != NULL) {
				sprintf(ttstr2, "%ld, %ld, %s \n", wid, nid, npath);
				fputs(ttstr2, fdd);
			}

			/* get the node's parents path */
			if ((pre_wid != wid) && (pre_wid != 0)) {

					ind = 0;
					/* free old pathst and allocate new one  */
					if (pathstr != 0) {
						pfree(pathstr);
						pathstr = 0;
					}
					path_con_len = kh_size(hpath);
					if (path_con_len > 0) {
						pathstr = palloc0(path_con_len * 16 + 16);

						/* include opening-endpoint and closing-endpoint path */
						ind += sprintf(pathstr, "\'%s\',\'%s\'", "0", "t");

						for (j = kh_begin(hpath) ; j != kh_end(hpath) ; j++) {
							if (kh_exist(hpath, j)) {
								ind += sprintf(&pathstr[ind], ",\'%s\'", kh_key(hpath, j));
							}
						}

						/* select the node_ids from the paths  */
						path2id = palloc0((80 + path_con_len) * sizeof(char));
						sprintf(path2id, path2id_tpl, pre_wid, pathstr);

						/* for debug purpose */
						if (fdd != NULL) {
							fputs (path2id, fdd);
							fputs ("\n", fdd);
						}

						ret2 = SPI_execute(path2id, true, 0);
						proc2 = SPI_processed;

						/* collect the way_trees node's necessary parents into hashset  */
						if (ret2 > 0 && SPI_tuptable != NULL) {
							TupleDesc tupdesc = SPI_tuptable->tupdesc;
							SPITupleTable * tuptable = SPI_tuptable;
							long wtnid  = 0;

							for (j = 0 ; j < proc2 ; j++) {
								HeapTuple tuple = tuptable->vals[j];

								wtnid = atol(SPI_getvalue(tuple, tupdesc, 1));
								kh_put(nid, hnid, wtnid, &abs);
							}
						}
						
						/* reinitialize all the temp variable */
						kh_clear(path, hpath);
					}
				}

				pre_wid = wid;
				get_ancestors(npath, hpath);
		}
	}

	/* free the allocated memories */
	kh_clear(str, h);
	kh_clear(path, hpath);

	//construct the array to be returned

	for (i = 0 ; i < proc ; ++i) {
		kh_put(nid, hnid, nids[i], &abs);
	}

	hnidlen = kh_size(hnid);
	vals = palloc0(sizeof(Datum) * hnidlen);
	count = 0;

	//a line to indicate the start of the result ids
	fputs ("start of the result ids\n", fdd);

	for (i = kh_begin(hnid) ; i != kh_end(hnid) ; ++i) {
		if (kh_exist(hnid, i)) {
			vals[count++] = Int64GetDatum(kh_key(hnid, i));
			/* for debug purpose */
			if (fdd != NULL) {
				sprintf(ttstr, "%ld\n", kh_key(hnid, i));
				fputs (ttstr, fdd);
			}
		}
	}

	retarr = construct_array(vals, hnidlen, INT8OID, sizeof(long), true, 'i');

	/* return the all directly and indirectly included nodes as response */
	//retarr = iinids2array(hnid, nids, proc);

	/* for debug purpose */
	if (fdd != NULL) {
		fclose(fdd);
	}

	SPI_finish();

	//free the memoried used in h
	for (i = 0 ; i < 80 ; ++i) {
		pfree(tiles[i]);
	}

	PG_RETURN_ARRAYTYPE_P(retarr);
	//PG_RETURN_ARRAYTYPE_P(construct_empty_array(INT8OID));
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
