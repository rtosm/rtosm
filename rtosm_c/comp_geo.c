#include "postgres.h"
#include "executor/spi.h"
#include "fmgr.h"
#include <utils/array.h>
#include <math.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include <string.h>

PG_MODULE_MAGIC;

Datum seg2pt_c(PG_FUNCTION_ARGS) ;
Datum gcode_c(PG_FUNCTION_ARGS) ;
Datum maxdis_c(PG_FUNCTION_ARGS) ;
Datum simpl_c(PG_FUNCTION_ARGS) ;

/* internal functions that only called by functions in this file */
float seg2pt(double x1, double y1, double x2, double y2, double x3, double y3) ;
float pt2pt(double x1, double y1, double x2, double y2) ;
int gcode(double lon, double lat, float err, char* tile) ;
int lc(char * c, char * p) ;
int rc(char * c, char * p) ;
float simpl(long nids[], int lons[], int lats[], int i, int j, char path[], float p, float errors[], char* paths[], int sizes[], char* tiles[]) ;
int arborway(long cur_way_id, long ids[], float errors[], char* paths[], int sizes[], char* tiles[], int num) ;

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
	int i, ret_ins, ret;
	char * stmnt_prefix = "insert into way_trees(way_id, node_id, error, path, subsize, tile) values (";
	char stmnt[160];

	ret = 0;

	for (i = 0 ; i < num ; i++) {
		memset (stmnt, 0, 160);
		sprintf(stmnt, "%s %lu, %lu, %f, \'%s\', %u,\'%s\')", stmnt_prefix, cur_way_id, ids[i], errors[i], paths[i], sizes[i], tiles[i]);

		ret_ins = SPI_exec(stmnt, 0);
		if (ret_ins < 0) {
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
