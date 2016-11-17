#include "postgres.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "khash.h"
#include "kvec.h"
#include <utils/array.h>
#include <math.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include <catalog/pg_type.h>
#include <string.h>
#include <stdio.h>
#include "heap.h"
#include "ebtree.h"

PG_MODULE_MAGIC;

/* external functions to be called by SQL statements */
Datum seg2pt_c(PG_FUNCTION_ARGS) ;
Datum gcode_c(PG_FUNCTION_ARGS) ;
Datum maxdis_c(PG_FUNCTION_ARGS) ;
Datum simpl_c(PG_FUNCTION_ARGS) ;
Datum wquery_c(PG_FUNCTION_ARGS) ;
Datum ntiles_c(PG_FUNCTION_ARGS) ;
Datum tileid_c(PG_FUNCTION_ARGS) ;
Datum vquery_c(PG_FUNCTION_ARGS) ;
Datum ntoe_c(PG_FUNCTION_ARGS) ;
Datum eton_c(PG_FUNCTION_ARGS) ;
Datum depth_c(PG_FUNCTION_ARGS) ;
Datum qtile_c(PG_FUNCTION_ARGS) ;

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


PG_FUNCTION_INFO_V1(gcode_c);

/* wrappered function gcode for PL/pgsql
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
	long * ids = 0, proc = 0;

	int ret, * lons, * lats;
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

PG_FUNCTION_INFO_V1(wquery_c);
/*
	the parameters is the coordinates of the leftbottom and topright point of the querying window

*/
Datum wquery_c(PG_FUNCTION_ARGS) {
	double wx1, wy1, wx2, wy2, elimit;
	khash_t(str) * h;
	khash_t(path) * hpath;
	khash_t(nid) * hnid;
	int i, j, ret, proc, ind, abs, count, hnidlen ;
	unsigned wx1uint, wy1uint, wx2uint, wy2uint;

	// limit the number of directly included node to 30000;
	//char * pre_stmt = "select way_id, node_id, path from way_trees where node_id in (select cn.id from (select node_id from node_vis_errors nve where (tile_level, tile) in (%s)) nve inner join current_nodes cn on cn.id = nve.node_id and cn.longitude >= %d and cn.longitude <= %d and cn.latitude >= %d and cn.latitude <= %d) order by way_id, path;";
	char * pre_stmt = "select way_id, node_id, path from way_trees where node_id in (select cn.id from (select node_id from node_vis_errors nve where (tile_level, tile) in (%s) and error > %f) nve inner join current_nodes cn on cn.id = nve.node_id and cn.longitude >= %d and cn.longitude <= %d and cn.latitude >= %d and cn.latitude <= %d) order by way_id, path;";
	char stmnt[2000] ;
	char tilestr[1500];

	long * wids, * nids;
	char ** paths ;
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

	elimit = wx2 - wx1;
	if (elimit < wy2 - wy1) {
		elimit = wy2 - wy1;
	}
	elimit = elimit / 2000 * 3;

	/* initialization of 3 hash sets
		1. for tiles conditions
		2. for paths of ancestors of a way's directly included nodes
		3. for id of indirectly included nodes
		*/
	h = kh_init(str);
	hpath = kh_init(path);
	hnid = kh_init(nid);

	/* Initialize char string for database querying */
	memset((void *)stmnt, 0, 2000 * sizeof(char));
	memset((void *)tilestr, 0, 1500 * sizeof(char));

	/* from window to tiles conditions  */
	window2tiles(wx1, wy1, wx2, wy2, h, tilestr);

	/* from tiles condition to directly included nodes, ways and relations */
	/* tiles2dinids(wx1, wy1, wx2, wy2, h, &wids, &nids, &paths);  */

	/* from directly included nodes into indirectly included nodes using way_trees and relation_trees */
	/* dinids2iinids(h, wids, nids, paths, hpath, hnid); */
		
	wx1uint = wx1 * 10000000;
	wy1uint = wy1 * 10000000;
	wx2uint = wx2 * 10000000;
	wy2uint = wy2 * 10000000;
	
	sprintf(stmnt, pre_stmt, tilestr, elimit, wx1uint, wx2uint, wy1uint, wy2uint);
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
		char *npath, * pathstr = NULL ;
		int path_con_len = 0, ret2, proc2 ;
		//get the nodes which are ancestor of a selected way's directly included nodes;
		char * path2id_tpl = "select node_id from way_trees where way_id = %lu and path in (%s)"; 
		char * path2id = NULL;
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
					if (pathstr != NULL) {
						pfree(pathstr);
						pathstr = NULL;
					}
					if (path2id != NULL) {
						pfree(path2id);
						path2id = NULL;
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
						path2id = palloc0(80 + path_con_len * 16 + 16);
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
	if (fdd) {
	 	fputs ("start of the result ids\n", fdd);
	}

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

	PG_RETURN_ARRAYTYPE_P(retarr);
	//PG_RETURN_ARRAYTYPE_P(construct_empty_array(INT8OID));
}

PG_FUNCTION_INFO_V1(ntiles_c);
/*
	build the hierarchical tile index for the nodes. after the execution of this function, the table of tile_nums is fulled.

*/
Datum ntiles_c(PG_FUNCTION_ARGS) {

	char * nes_stmnt = "select ne.node_id, ne.error, ns.longitude, ns.latitude from node_err ne inner join nodes ns on ns.node_id = ne.node_id order by ne.error DESC" ;
	int ret ;
	int node_limit = PG_GETARG_INT32(0) ;
	long proc ;

	/* begin to query the database to retrieve the data */
	SPI_connect();

	ret = SPI_execute(nes_stmnt, true, 0);

	proc = SPI_processed;

	/* for an empty results */
	if (0 == proc) {
		SPI_finish();
		PG_RETURN_INT32(0);
	}

	/* read out the actual data */
	if (ret > 0 && SPI_tuptable != NULL) {
		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		SPITupleTable * tuptable = SPI_tuptable;

		long nid = 0;
		unsigned long geohash_bits = 0, geohash_children[4];
		float ne = 0, lonf = 0, latf = 0, xlbound = -180, xubound = 180, ylbound = -90, yubound = 90;
		int lon, lat, bits_count = 0, level_p = 0, auxi_p[20], ret2, ret3, i;
		char * ins_stmnt, * ins_tmpl = "insert into node_vis_errors values (%lu, %f, %u, \'%s\')" ; 
		char * ins_tn_stmnt, * ins_tn_tmpl = "insert into tile_nums values (%u, \'%s\', %u, %f, %f)" ;
		qtree * qroot = palloc(sizeof(qtree));
		qtree * qnode = qroot;
		qtree * stck[20];
		FILE * fdd = fopen("/tmp/ntiles.txt", "a");

		ins_stmnt = palloc0(128);
		ins_tn_stmnt = palloc0(128);

		qroot->tile[0] = '0' ;
		qroot->count = 0 ;
		qroot->children[0] = NULL; qroot->children[1] = NULL; qroot->children[2] = NULL; qroot->children[3] = NULL;
		qroot->geohash_bits = 0; qroot->ubound = -1 ; qroot->lbound = -1;

		/* copy the data retrieved from table into arrays  */
		for (i = 0 ; i < proc; i++) {
			HeapTuple tuple = tuptable->vals[i];
			
			nid = atol(SPI_getvalue(tuple, tupdesc, 1));
			ne = atof(SPI_getvalue(tuple, tupdesc, 2));
			lon = atoi(SPI_getvalue(tuple, tupdesc, 3));
			lat = atoi(SPI_getvalue(tuple, tupdesc, 4));

			lonf = lon / 10000000.0 ;
			latf = lat / 10000000.0 ;

			xlbound = -180; xubound = 180; ylbound = -90; yubound = 90;
			geohash_bits = 0; bits_count = 0;
			qnode = qroot;

			while (qnode->count >= node_limit) {
					// binary search for lon and lat and select the children node as the qnode
					if (!qnode->children[0]) {
						qnode->children[0] = palloc(sizeof(qtree));
						memset(qnode->children[0], 0, sizeof(qtree));
						geohash_children[0] = (geohash_bits << 2) ;
						qnode->children[0]->ubound = -1; qnode->children[0]->lbound = -1;
						gcode2(geohash_children[0], bits_count + 1, qnode->children[0]->tile);

						qnode->children[1] = palloc(sizeof(qtree));
						memset(qnode->children[1], 0, sizeof(qtree));
						geohash_children[1] = (geohash_bits << 2) + 0x01;
						qnode->children[1]->ubound = -1; qnode->children[1]->lbound = -1;
						gcode2(geohash_children[1], bits_count + 1, qnode->children[1]->tile);

						qnode->children[2] = palloc(sizeof(qtree));
						memset(qnode->children[2], 0, sizeof(qtree));
						geohash_children[2] = (geohash_bits << 2) + 0x02;
						qnode->children[2]->ubound = -1; qnode->children[2]->lbound = -1;
						gcode2(geohash_children[2], bits_count + 1, qnode->children[2]->tile);

						qnode->children[3] = palloc(sizeof(qtree));
						memset(qnode->children[3], 0, sizeof(qtree));
						geohash_children[3] = (geohash_bits << 2) + 0x03;
						qnode->children[3]->ubound = -1; qnode->children[3]->lbound = -1;
						gcode2(geohash_children[3], bits_count + 1, qnode->children[3]->tile);
					}

					if (lonf < (xlbound + xubound) / 2) {
						geohash_bits = (geohash_bits << 1) & 0xfffffffffffffffeull;
						xubound = (xlbound + xubound) / 2;
					} else {
						geohash_bits = (geohash_bits << 1) | 0x0000000000000001ull;
						xlbound = (xlbound + xubound) / 2;
					}

					if (latf < (ylbound + yubound) / 2) {
						geohash_bits = (geohash_bits << 1) & 0xfffffffffffffffeull;
						yubound = (ylbound + yubound) / 2;
					} else {
						geohash_bits = (geohash_bits << 1) | 0x0000000000000001ull;
						ylbound = (ylbound + yubound) / 2;
					}

					qnode = qnode->children[geohash_bits & 0x0000000000000003ull];
					bits_count++;
			}

			qnode->count++;
			if (qnode->ubound < 0) qnode->ubound = ne;
			qnode->lbound = ne;

			// insert the node_id, error, tile into the node_vis_errors
			sprintf(ins_stmnt, ins_tmpl, nid, ne, bits_count, qnode->tile);
			ret2 = SPI_exec(ins_stmnt, 0);
			if (ret2 < 0) {
				//error in inserting, logging some stuff
				if (fdd) {
					fprintf(fdd, "node_vis_errors insertion failed with SQL: \'%s\' \n", ins_stmnt);
				}
			}
		}

		//test the contents of qtree 
		dump2file(qroot, fdd, 0);

		//after the insertions of node_vis_errors, insert the tile_nums
			// traverse the quad-tree to retrieve the number of node in each tile
			// a manually crafted stack used here to avoid using recursive function.
		stck[0] = qroot;

		memset(auxi_p, 0, 20 * sizeof (*auxi_p));
		while (level_p >= 0) {
			qnode = stck[level_p];
			if (fdd) {
				fprintf(fdd, "level_p: %d ; auxi_p[level_p]: %d ; count : %d \n", level_p, auxi_p[level_p], qnode->count);
			}
			//insert the tile_nums row
			if (qnode->count > 0) {
				if (auxi_p[level_p] == 0) {
					sprintf(ins_tn_stmnt, ins_tn_tmpl, level_p, stck[level_p]->tile, stck[level_p]->count, stck[level_p]->ubound, stck[level_p]->lbound) ;
					ret3 = SPI_exec(ins_tn_stmnt, 0);
					if (fdd && ret3 > 0) {
						fputs(ins_tn_stmnt, fdd);
						fputs("\n", fdd);
					}

					if (qnode->children[0]) {
						stck[level_p + 1] = qnode->children[auxi_p[level_p]];
						auxi_p[level_p]++;
						auxi_p[level_p + 1] = 0;
						level_p++;
					} else {
						pfree(stck[level_p]);
						//return to parent node
						level_p--;
					}
				} else if (auxi_p[level_p] <= 3) {
					if (qnode->children[0]) {
						stck[level_p + 1] = qnode->children[auxi_p[level_p]];
						auxi_p[level_p]++;
						auxi_p[level_p + 1] = 0;
						level_p++;
					} else {
						pfree(stck[level_p]);
						//return to parent node
						level_p--;
					}

				} else {
					pfree(stck[level_p]);
					level_p--;
				}
			} else {
				pfree(stck[level_p]);
				level_p--;
			}
		}
		pfree(ins_stmnt);
		pfree(ins_tn_stmnt);
		if (fdd) fclose(fdd);

	}

	SPI_finish();

	PG_RETURN_INT64(proc);
}

PG_FUNCTION_INFO_V1(tileid_c);
/*
	build the hierarchical tile index for the nodes. after the execution of this function, the table of tile_nums is fulled.
		another implementation of hierarchical tile index building along with ntiles_c using SPI_cursor_fetch to avoid memory overflow.

*/
Datum tileid_c(PG_FUNCTION_ARGS) {

	char * pre_cursor = "select ne.node_id, ne.error, ns.longitude, ns.latitude from node_err ne inner join nodes ns on ns.node_id = ne.node_id order by ne.error DESC";
	long gulp = PG_GETARG_INT32(0), proc, node_count = 0;

	SPIPlanPtr pptr;
	Portal prtl;
	
	SPI_connect();

	pptr = SPI_prepare_cursor(pre_cursor, 0, NULL, CURSOR_OPT_NO_SCROLL);

	prtl = SPI_cursor_open(NULL, pptr, NULL, NULL, true);

	SPI_cursor_fetch(prtl, true, gulp);

	proc = SPI_processed;

	if (proc > 0 && SPI_tuptable != NULL) {
		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		SPITupleTable * tuptable = SPI_tuptable;

		long nid = 0;
		unsigned long geohash_bits = 0, geohash_children[4];
		float ne = 0, lonf = 0, latf = 0, xlbound = -180, xubound = 180, ylbound = -90, yubound = 90;
		int lon, lat, bits_count = 0, level_p = 0, auxi_p[20], ret2, ret3, i;
		char * ins_stmnt, * ins_tmpl = "insert into node_vis_errors values (%lu, %f, %u, \'%s\')" ; 
		char * ins_tn_stmnt, * ins_tn_tmpl = "insert into tile_nums values (%u, \'%s\', %u, %f, %f)" ;
		qtree * qroot = palloc(sizeof(qtree));
		qtree * qnode = qroot;
		qtree * stck[20];
		FILE * fdd = fopen("/tmp/tileid.txt", "a");

		ins_stmnt = palloc0(128);
		ins_tn_stmnt = palloc0(128);

		qroot->tile[0] = '0' ; qroot->tile[1] = '\0';
		qroot->count = 0 ;
		qroot->children[0] = NULL; qroot->children[1] = NULL; qroot->children[2] = NULL; qroot->children[3] = NULL;
		qroot->geohash_bits = 0; qroot->ubound = -1 ; qroot->lbound = -1;

		do {
			node_count += proc;
			for (i = 0 ; i < proc; i++) {
				HeapTuple tuple = tuptable->vals[i];
				
				nid = atol(SPI_getvalue(tuple, tupdesc, 1));
				ne = atof(SPI_getvalue(tuple, tupdesc, 2));
				lon = atoi(SPI_getvalue(tuple, tupdesc, 3));
				lat = atoi(SPI_getvalue(tuple, tupdesc, 4));

				lonf = lon / 10000000.0 ;
				latf = lat / 10000000.0 ;

				xlbound = -180; xubound = 180; ylbound = -90; yubound = 90;
				geohash_bits = 0; bits_count = 0;
				qnode = qroot;

				while (qnode->count >= gulp) {
						// binary search for lon and lat and select the children node as the qnode
						if (!qnode->children[0]) {
							qnode->children[0] = palloc(sizeof(qtree));
							memset(qnode->children[0], 0, sizeof(qtree));
							geohash_children[0] = (geohash_bits << 2) ;
							qnode->children[0]->ubound = -1; qnode->children[0]->lbound = -1;
							gcode2(geohash_children[0], bits_count + 1, qnode->children[0]->tile);

							qnode->children[1] = palloc(sizeof(qtree));
							memset(qnode->children[1], 0, sizeof(qtree));
							geohash_children[1] = (geohash_bits << 2) + 0x01;
							qnode->children[1]->ubound = -1; qnode->children[1]->lbound = -1;
							gcode2(geohash_children[1], bits_count + 1, qnode->children[1]->tile);

							qnode->children[2] = palloc(sizeof(qtree));
							memset(qnode->children[2], 0, sizeof(qtree));
							geohash_children[2] = (geohash_bits << 2) + 0x02;
							qnode->children[2]->ubound = -1; qnode->children[2]->lbound = -1;
							gcode2(geohash_children[2], bits_count + 1, qnode->children[2]->tile);

							qnode->children[3] = palloc(sizeof(qtree));
							memset(qnode->children[3], 0, sizeof(qtree));
							geohash_children[3] = (geohash_bits << 2) + 0x03;
							qnode->children[3]->ubound = -1; qnode->children[3]->lbound = -1;
							gcode2(geohash_children[3], bits_count + 1, qnode->children[3]->tile);
						}

						if (lonf < (xlbound + xubound) / 2) {
							geohash_bits = (geohash_bits << 1) & 0xfffffffffffffffeull;
							xubound = (xlbound + xubound) / 2;
						} else {
							geohash_bits = (geohash_bits << 1) | 0x0000000000000001ull;
							xlbound = (xlbound + xubound) / 2;
						}

						if (latf < (ylbound + yubound) / 2) {
							geohash_bits = (geohash_bits << 1) & 0xfffffffffffffffeull;
							yubound = (ylbound + yubound) / 2;
						} else {
							geohash_bits = (geohash_bits << 1) | 0x0000000000000001ull;
							ylbound = (ylbound + yubound) / 2;
						}

						qnode = qnode->children[geohash_bits & 0x0000000000000003ull];
						bits_count++;
				}

				qnode->count++;
				if (qnode->ubound < 0) qnode->ubound = ne;
				qnode->lbound = ne;

				// insert the node_id, error, tile into the node_vis_errors
				sprintf(ins_stmnt, ins_tmpl, nid, ne, bits_count, qnode->tile);
				ret2 = SPI_exec(ins_stmnt, 0);
				if (ret2 < 0) {
					//error in inserting, logging some stuff
					if (fdd) {
						fprintf(fdd, "node_vis_errors insertion failed with SQL: \'%s\' \n", ins_stmnt);
					}
				}
			}

			SPI_cursor_fetch(prtl, true, gulp);

			proc = SPI_processed;

			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;

		} while (proc > 0) ;

		//test the contents of qtree 
		dump2file(qroot, fdd, 0);

		//after the insertions of node_vis_errors, insert the tile_nums
			// traverse the quad-tree to retrieve the number of node in each tile
			// a manually crafted stack used here
		stck[0] = qroot;

		memset(auxi_p, 0, 20 * sizeof (*auxi_p));
		while (level_p >= 0) {
			qnode = stck[level_p];
			if (fdd) {
				fprintf(fdd, "level_p: %d ; auxi_p[level_p]: %d ; count : %d \n", level_p, auxi_p[level_p], qnode->count);
			}
			//insert the tile_nums row
			if (qnode->count > 0) {
				if (auxi_p[level_p] == 0) {
					sprintf(ins_tn_stmnt, ins_tn_tmpl, level_p, stck[level_p]->tile, stck[level_p]->count, stck[level_p]->ubound, stck[level_p]->lbound) ;
					ret3 = SPI_exec(ins_tn_stmnt, 0);
					if (fdd && ret3 > 0) {
						fputs(ins_tn_stmnt, fdd);
						fputs("\n", fdd);
					}

					if (qnode->children[0]) {
						stck[level_p + 1] = qnode->children[auxi_p[level_p]];
						auxi_p[level_p]++;
						auxi_p[level_p + 1] = 0;
						level_p++;
					} else {
						pfree(stck[level_p]);
						//return to parent node
						level_p--;
					}
				} else if (auxi_p[level_p] <= 3) {
					if (qnode->children[0]) {
						stck[level_p + 1] = qnode->children[auxi_p[level_p]];
						auxi_p[level_p]++;
						auxi_p[level_p + 1] = 0;
						level_p++;
					} else {
						pfree(stck[level_p]);
						//return to parent node
						level_p--;
					}

				} else {
					pfree(stck[level_p]);
					level_p--;
				}
			} else {
				pfree(stck[level_p]);
				level_p--;
			}
		}

		pfree(ins_stmnt);
		pfree(ins_tn_stmnt);
		if (fdd) fclose(fdd);
		SPI_finish();

	} else {
		SPI_finish();
		PG_RETURN_INT64(0);
	}

	PG_RETURN_INT64(node_count);
}


PG_FUNCTION_INFO_V1(ntoe_c);

// query to get the error lower bound of top n nodes in a target area
	//utilize the recursive function to count the top n nodes;
Datum ntoe_c(PG_FUNCTION_ARGS) {
	double wx1 = PG_GETARG_FLOAT8(0);
	double wy1 = PG_GETARG_FLOAT8(1);
	double wx2 = PG_GETARG_FLOAT8(2);
	double wy2 = PG_GETARG_FLOAT8(3);

	int nlimit = PG_GETARG_INT32(4), ncount = 0;
	float elimit = -1;
	
	Tile_tree * troot = palloc0(sizeof(Tile_tree)) ;
	Tile_tree * max_digger = NULL;
	float * max_elb ;
	heap h;
	bool isOverlapped = false;

	heap_create(&h, 0, compare_tt_keys);

	//create the SPI environment to retrieve data from database
	SPI_connect();

	read_tile("0", 0, troot);
	heap_infix(&h, (void *)&troot->elb, (void *)troot);

	while (ncount < nlimit && heap_size(&h) > 0) {
		heap_delmin(&h, (void **)&max_elb, (void **)&max_digger);
		ncount += covered_nodes_number(max_digger, wx1, wy1, wx2, wy2, &isOverlapped);
		if (isOverlapped) dig_down(&h, max_digger);
		elimit = *max_elb;
	}

	SPI_finish();

	PG_RETURN_FLOAT4(elimit);
}


PG_FUNCTION_INFO_V1(eton_c);

// query to get the number of nodes which have error at least err in a target area
Datum eton_c(PG_FUNCTION_ARGS) {

	double wx1 = PG_GETARG_FLOAT8(0);
	double wy1 = PG_GETARG_FLOAT8(1);
	double wx2 = PG_GETARG_FLOAT8(2);
	double wy2 = PG_GETARG_FLOAT8(3);

	float elimit = PG_GETARG_FLOAT4(4), erecord = 361;
	int nlimit = 0, ncount = 0;
	
	Tile_tree * troot = palloc0(sizeof(Tile_tree)) ;
	Tile_tree * max_digger = NULL;
	float * max_elb ;
	heap h;
	bool isOverlapped;

	//create the heap
	heap_create(&h, 0, compare_tt_keys);

	//create the SPI environment to retrieve data from database
	SPI_connect();

	read_tile("0", 0, troot);
	heap_infix(&h, (void *)&troot->elb, (void *)troot);

	while (erecord > elimit && heap_size(&h) > 0) {
		heap_delmin(&h, (void **)&max_elb, (void **)&max_digger);
		ncount = nlimit ;
		nlimit += covered_nodes_number(max_digger, wx1, wy1, wx2, wy2, &isOverlapped);
		if (nlimit > ncount) {
			erecord = max_digger->elb;
		}
		if (isOverlapped) dig_down(&h, max_digger);
	}

	SPI_finish();

	PG_RETURN_INT32(nlimit);
}

//two hashmap to handle the tile_str numbers and find the max-min error in tile nodes
//KHASH_MAP_INIT_STR(tlstr, float);
//KHASH_MAP_INIT_STR(mami, long);
//KHASH_SET_INIT_INT64(nodeid);

PG_FUNCTION_INFO_V1(vquery_c);
/*
	user specify the query condition including the querying window and error tolerance and count limit
*/
Datum vquery_c(PG_FUNCTION_ARGS) { 
	int ncount = 0, cur_depth = 0, depth_limit = 0; //, rs_num = 0, ret, proc, i, inode_num, retcnt = 0;
	int i, inode_num, retcnt = 0;

	//get arguments 
	double wx1 = PG_GETARG_FLOAT8(0);
	double wy1 = PG_GETARG_FLOAT8(1);
	double wx2 = PG_GETARG_FLOAT8(2);
	double wy2 = PG_GETARG_FLOAT8(3);
	int nlimit = PG_GETARG_INT32(4);
	float elimit = PG_GETARG_FLOAT4(5);

	//char tile_nodes_str[512], *hm_key;

	//array to hold indirectly included nodes
	long * inodes, * rst ;
	//a hashset for the nodes in case of duplicated node

	//max-min error for 20 levels of tiles
	float mami[20];
	// max-min top index for tile nodes ; numbers of tile nodes
	int lv_nd_mmtop[20], lv_nd_nums[20] ;
	//int lv_tl_nums[20], lv_nd_remains[20], lv_nd_nums[20] ;
	//query results for 20 levels of tiles
	presort_node *pns[20] ;
	//for debug purpose
	FILE * fdd = fopen("/tmp/palloc.txt","a");

	//khash_t(mami) * hmm = kh_init(mami) ;
	//khash_t(nodeid) * hnid = kh_init(nodeid);

	//depress the warning of unused elimit 
	mami[0] = elimit;

	//return array's elements
	Datum * vals ;
	ArrayType * retarr;

	//heap h;
	//heap_create(&h, 0, compare_tt_keys);

	depth_limit = depth(wx1, wy1, wx2, wy2);

	//sprintf (tile_nodes_str, tile_tmpl, 0, "0", wx1int, wx2int, wy1int, wy2int, nlimit);

	SPI_connect();

	//int wx1int = wx1 * 10000000, wy1int = wy1 * 10000000, wx2int = wx2 * 10000000, wy2int = wy2 * 10000000;
	if (fdd) {
		fprintf(fdd, "rst allocated memory %ld\n", nlimit * sizeof(long));
		fflush(fdd);
	}
	rst = palloc0(nlimit * sizeof (long));

	//retrieve nodes from the top tile 
	//ret = SPI_execute(tile_nodes_str, true, 0);

	//proc = SPI_processed ;

	while (ncount < nlimit && cur_depth <= depth_limit) {

		//from coordinate to the level nodes;
		get_nodes(wx1, wy1, wx2, wy2, cur_depth, &pns[cur_depth], nlimit - ncount, &lv_nd_nums[cur_depth],&lv_nd_mmtop[cur_depth], &mami[cur_depth]);

		//get the level nodes;
		//xy2tilestr(tile_nodes_str, &lv_tl_nums[cur_depth], hme);

		//get the level's max-min error
		//mami[i] = get_mami(&ncount, resultnid, lv_nd_remains, &lv_nd_nums, cur_depth);

		//extract all nodes in pns with error larger than mami error to resultset
		ncount += set_result(rst, ncount, nlimit, pns, cur_depth, lv_nd_nums, lv_nd_mmtop, mami);

		if (fdd) {
			fprintf(fdd, "The number of nodes in result sets is %d ;\n", ncount);
			fflush(fdd);
		}

		cur_depth++;
	}

	//guard following case:
		//1. dig to the floor(level = 20) and resultset is not full or even empty
		//2. very easily full

	// indirectly included nodes by get ebtree ancestor of each relation/way
	//char * ways_str = "select wt.way_id, wt.node_id, wt.path from way_trees wt where wt.node_id in (%s) order by wt.way_id, wt.path";
	get_more_nodes(rst, ncount, &inodes, &inode_num);

	//construct an array of node_id to return
	if (fdd) {
		fprintf(fdd, "the vals allocated size %ld\n", sizeof(Datum) * (ncount+inode_num));
		fflush(fdd);
	}
	vals = palloc0(sizeof(Datum) * (ncount + inode_num));

	for (i = 0 ; i < ncount ; i++) {
		vals[retcnt++] = Int64GetDatum(rst[i]);
	}

	for (i = 0 ; i < inode_num ; i++) {
		vals[retcnt++] = Int64GetDatum(inodes[i]);
	}
	//eliminate the dulications in result set


	retarr = construct_array(vals, retcnt, INT8OID, sizeof(long), true, 'i');
	if (fdd) {
		fprintf(fdd, "the return array built size %d\n", retcnt);
		fflush(fdd);
	}

	//free the allocated memories
	//pfree(rst);
	//pfree(inodes);

	//free the presort_node's array
	for (i = 0 ; i < cur_depth ; i++) {
		//pfree(pns[i]);
	}
	if (fdd) {
		fprintf(fdd, "before SPI_finish()\n");
	}

	SPI_finish();

	if (fdd) {
		fprintf(fdd, "after SPI_finish()\n");
		fclose(fdd);
	}

	PG_RETURN_ARRAYTYPE_P(retarr);

	//PG_RETURN_INT32(ncount + inode_num);
}

PG_FUNCTION_INFO_V1(depth_c);
/*
	user specify the query condition including the querying window and error tolerance and count limit
*/
Datum depth_c(PG_FUNCTION_ARGS) { 
	double wx1 = PG_GETARG_FLOAT8(0);
	double wy1 = PG_GETARG_FLOAT8(1);
	double wx2 = PG_GETARG_FLOAT8(2);
	double wy2 = PG_GETARG_FLOAT8(3);

	PG_RETURN_INT32(depth(wx1, wy1, wx2, wy2));
}

PG_FUNCTION_INFO_V1(qtile_c);
/*
	user specify the query condition including the querying window and error tolerance and count limit
*/
Datum qtile_c(PG_FUNCTION_ARGS) { 
	int level = PG_GETARG_INT32(0);
	double wx1 = PG_GETARG_FLOAT8(1);
	double wy1 = PG_GETARG_FLOAT8(2);
	double wx2 = PG_GETARG_FLOAT8(3);
	double wy2 = PG_GETARG_FLOAT8(4);

	// val[i] = CStringGetTextDatum();
	// see the construct_array in Postgresql source

	char *qtiles[4];
  Datum datstr[4] ;
	ArrayType * retstr;
  int i ;

	for (i = 0 ; i < 4 ; i++) 
		qtiles[i] = palloc0(9 * sizeof(char));

	gbits2tile(xy2gbits(wx1, wy1, level), level, qtiles[0]) ;
	gbits2tile(xy2gbits(wx1, wy2, level), level, qtiles[1]) ;
	gbits2tile(xy2gbits(wx2, wy2, level), level, qtiles[2]) ;
	gbits2tile(xy2gbits(wx2, wy1, level), level, qtiles[3]) ;

  for (i = 0 ; i < 4 ; i++) {
    datstr[i] = CStringGetTextDatum(qtiles[i]);
  }

  retstr = construct_array(datstr, 4, TEXTOID, -1, false, 'i');

  PG_RETURN_ARRAYTYPE_P(retstr);
}

int get_nodes(double wx1, double wy1, double wx2, double wy2, int level, presort_node **pns, int remain_len, int * nodes_num, int *mami_num, float * mami_err) {

	int wx1i = wx1 * 10000000;
	int wy1i = wy1 * 10000000;
	int wx2i = wx2 * 10000000;
	int wy2i = wy2 * 10000000;

	char qtiles[4][9];
  int i, rabs, j = 0, ret, proc, tile_num;

	char * tile_tmpl = "select nve.node_id, nve.error, nve.tile from node_vis_errors nve inner join nodes ns on nve.node_id = ns.node_id where nve.tile_level = %d and nve.tile in (%s) and ns.longitude >= %d and ns.longitude <= %d and ns.latitude >= %d and ns.latitude <= %d order by nve.error DESC limit %d";
	char tile_nodes_str[512], tilestr[64];
	//hashmap for tiles and their indexes of node with max-min error
	khash_t(tlstr) * hts = kh_init(tlstr) ;

	FILE * fdd = fopen("/tmp/palloc.txt", "a");

	//for (i = 0 ; i < 4 ; i++) 
		//qtiles[i] = palloc0(9 * sizeof(char));

	gbits2tile(xy2gbits(wx1, wy1, level), level, qtiles[0]) ;
	gbits2tile(xy2gbits(wx1, wy2, level), level, qtiles[1]) ;
	gbits2tile(xy2gbits(wx2, wy2, level), level, qtiles[2]) ;
	gbits2tile(xy2gbits(wx2, wy1, level), level, qtiles[3]) ;

	for (i = 0 ; i < 4 ; i++) {
		int ki ;
		ki = kh_put(tlstr, hts, qtiles[i], &rabs);
		if (rabs) kh_val(hts, ki) = -1;
	}

	tile_num = kh_size(hts);
	if (fdd) {
		fprintf(fdd, "the tile_num is %d\n", tile_num);
		fflush(fdd);
	}
	for (i = kh_begin(hts); i != kh_end(hts); i++) {
		if (kh_exist(hts, i)) {
			j += sprintf(tilestr + j, "\'%s\',", kh_key(hts, i));
		}
	}

	if (j > 0)
		tilestr[j - 1] = '\0';

	sprintf(tile_nodes_str, tile_tmpl, level, tilestr, wx1i, wx2i, wy1i, wy2i, remain_len);
	
	if (fdd) {
		fprintf (fdd, "The level %d nodes query string is %s ; \n", level, tile_nodes_str);
		fflush(fdd);
	}
	ret = SPI_execute(tile_nodes_str, true, 0);

	proc = SPI_processed;
	*nodes_num = proc;

	if (ret > 0 && SPI_tuptable != NULL) {
		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		SPITupleTable * tuptable = SPI_tuptable;
		presort_node *pna ;
		// the number of founded max-min error of each tile
		int mmn = 0;

		if (fdd) fprintf(fdd, "pns allocated size : %ld \n", proc * sizeof(presort_node)); 
		if (fdd) fflush(fdd);
		*pns = palloc(proc * sizeof(presort_node));

		pna = *pns;
		*mami_num = 0;
		*mami_err = -1;

		for (i = proc - 1 ; i >= 0 ; i--) {
			HeapTuple tuple = tuptable->vals[i];

			pna[i].nid = atol(SPI_getvalue(tuple, tupdesc, 1));
			pna[i].error = atof(SPI_getvalue(tuple, tupdesc, 2));
			pna[i].tile = SPI_getvalue(tuple, tupdesc, 3);

			//trim the space in tile to ensure the equality of calculated tile string
			for (j = 1 ; j < 9 ; j++) {
				if (pna[i].tile[j] == ' ') {
					pna[i].tile[j] = '\0';
					break;
				} 
			}
			if (fdd) {
				fprintf(fdd, "nid: %lu ; error : %f ; tile : %s \n", pna[i].nid, pna[i].error, pna[i].tile);
				fflush(fdd);
			}

			//find the max-min error in the level tiles
			if (mmn < tile_num) {
				int ki;
				ki = kh_get(tlstr, hts, pna[i].tile);
				if (fdd) {
					fprintf(fdd, "The kh_val(hts,ki) is %d\n", kh_val(hts, ki));
					fprintf(fdd, "ki : %d ; kend : %d \n", ki, kh_end(hts));
					fflush(fdd);
				}
				if (ki != kh_end(hts) && kh_val(hts, ki) < 0) {
					//store the index of the node with max-min error for each of the 4 tiles
					kh_val(hts, ki) = i + 1;
					mmn++;

					//if all the indexes of the nodes with max-min error for the 4 tiles
						//then set the level's max-min error and index 
					if (mmn == tile_num) {
						*mami_err = pna[i].error;
						*mami_num = i + 1;
						if (fdd) {
							fprintf(fdd, "The level : %d mami_num : %d , mami_error %f ; \n", level, *mami_num, *mami_err);
							fflush(fdd);
						}
					}
				}
			}
		}

		//guard for some tiles are empty
		if (mmn < tile_num) {
			//iterator over the existing max-min error of tile nodes
			for (i = kh_begin(hts) ; i != kh_end(hts) ; i++) {
				if (kh_exist(hts, i) && (kh_val(hts, i) > *mami_num)) {
					*mami_num = kh_val(hts, i) + 1;
					*mami_err = pna[*mami_num - 1].error;
					if (fdd) {
						fprintf(fdd, "The kh_val(hts,ki) is %d\n", kh_val(hts, i));
						fflush(fdd);
					}
				}
			}
		}

		//kh_clear(tlstr, hts);
		kh_destroy(tlstr, hts);
		if (fdd) fclose(fdd);

	} else {
		// here query goes wrong
	}

	//clear the khash

	return 0;
}

//copy tile nodes into result set 
	// rst_top points to the top available resultset position
	// topmm points to the top remaining node, when no remaining node, the topmm equals tln_num
int set_result(long * rst, int rst_top, int nlimit, presort_node ** pns, int level, int * tln_num, int *topmm, float *mamis) {

	int i, j, rst_ts = rst_top;
	int candi_num = 0;
	int tmp_topmm[20];

	// test if the number of all candicate nodes overflows the resultset
	for (i = 0; i < level ; i++) {

		if (topmm[i] >= tln_num[i]) {
			// all the nodes have been copyed into resultset. 
			tmp_topmm[i] = topmm[i];
		} else {
			// Determine the number of nodes whose error is larger than current level's mami
			int lb = topmm[i];
			int ub = tln_num[i] - 1;
			int ct = (lb + ub) / 2;

			//guard for the edge case when all remaining node are larger/smaller than the max-min error
				// or the ub and lb are the same node where binary search doesn't work
			if (pns[i][ub].error >= mamis[level]) {
				candi_num += ub - lb + 1;
				tmp_topmm[i] = ub + 1;

			// prevent when no nodes are larger than the max-min error
			} else if (pns[i][lb].error < mamis[level]) {
				tmp_topmm[i] = lb;

			} else {

				//binary search for the mami nodes
				while (ub - lb > 1) {
					ct = (lb + ub) / 2;
					if (pns[i][ct].error > mamis[level]) {
						lb = ct;
					} else if (pns[i][ct].error < mamis[level]) {
						ub = ct;
					} else {
						ub = ct + 1;
						break;
					}
				}

				//calculate the increasement of the number of candidate nodes
				candi_num += ub - lb;
				tmp_topmm[i] = ub;
			}
		}
		//
	}

	// add the current level candidate nodes
	candi_num += topmm[level];

	if (rst_top + candi_num <= nlimit) {
		// simply copy from candidate set into result set
		for (i = 0 ; i < level ; i++) {
			for (j = topmm[i] ; j < tmp_topmm[i] ; j++) {
				rst[rst_top++] = pns[i][j].nid;
			}
			//update the top_max_min indicator for each level tile
			topmm[i] = tmp_topmm[i];
		}
		// copy the current level's nodes
		for (j = 0; j < topmm[level]; j++) {
			rst[rst_top++] = pns[level][j].nid;
		}
		//TODO: feedback to caller all nodes have been copied into resultset

	} else {
		// use a max-heap to push and pop out nodes from candidate set in order
		heap h;

			//first initiate a heap
		heap_create(&h, candi_num, compare_tt_keys);

		for (i = 0 ; i < level ; i++) {
			for (j = topmm[i] ; j < tmp_topmm[i] ; j++) {
				heap_infix(&h, (void *)&pns[i][j].error, (void *)&pns[i][j]);
			}
		}
		// copy the current level's nodes
		for (j = 0; j < topmm[level]; j++) {
			heap_infix(&h, (void *)&pns[level][j].error, (void *)&pns[level][j]);
		}

		//pop out nodes from max-heap
		while (nlimit - rst_top > 0) {
			float *key ;
			presort_node *pn;
			
			heap_delmin(&h, (void **)&key, (void **)&pn); 
			//exit the loop if heap is empty, this can't happen 
			//if (r == 0) break;
			rst[rst_top++] = pn->nid;
		}
		//deinitialize the heap
		heap_destroy(&h);
	}
	return rst_top - rst_ts;
}

//get the indirectly included nodes by get ancestor of directly included nodes;
int get_more_nodes(long * rst, int rst_num, long **inodeids, int * inode_num) {

	int ret, proc, i, j = 0, rabs, print_size, qstr_size = 0, ret2, proc2, way_count = 0;
	char * qtmpl = "select wt.way_id, wt.path from way_trees wt where wt.node_id in (%s) order by wt.way_id, wt.path";
	char * nodeidstr, * query_str, * path, *wn_paths = 0, *wn_qstr = 0 ;
	long wid, prewid = -1;
	//a dynamic array to hold ways' indirectly included nodes;
		// pairs of values such as (way_id, path)
	kvec_t(char *) indqstr;

	//for debug purpose
	FILE * fdd = fopen("/tmp/palloc.txt", "a");

	//hashset for directly-included nodes' ancestors pathes;
	khash_t(path) *hpath;
	hpath = kh_init(path);

	kv_init(indqstr);

	//hashset for directly-included nodes' path 
	khash_t(dnp) *hdnp ;
	hdnp = kh_init(dnp);

	if (fdd) fprintf(fdd, "the nodeidstr allocated size: %ld ; \nThe query_str allocated size %ld\n", rst_num * 12 + sizeof(char), (rst_num * 12 + 256) * sizeof(char));
	if (fdd) fflush(fdd);
	nodeidstr = palloc0(rst_num * 12 * sizeof(char));
	query_str = palloc0((rst_num * 12 + 256) * sizeof(char));

	//guard the edge case
	if (rst_num == 0) {
		*inodeids = NULL;
		*inode_num = 0;
		return *inode_num;
	}

	for (i = 0 ; i < rst_num ; i++) {
		j += sprintf(nodeidstr + j, "%lu,", rst[i]);
	}

	if (j > 0)
		nodeidstr[j - 1] = '\0';

	sprintf(query_str, qtmpl, nodeidstr);

	if (fdd) {
		fprintf(fdd, "The way nodes sorting query string : %s ;\n", query_str); 
		fflush(fdd);
	}

	ret = SPI_execute(query_str, true, 0);

	proc = SPI_processed;

	//free the allocated memory for query strings
	//pfree(nodeidstr);
	//pfree(query_str);

	if (ret > 0 && SPI_tuptable != NULL) {
		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		SPITupleTable * tuptable = SPI_tuptable;

		//allocate memory for the way_id, node_id and path
		//wids = palloc0(proc * sizeof(long));
		//nids = palloc0(proc * sizeof(long));
		//paths = palloc0(proc * sizeof(char *));
		//wn_num = proc;

		for (i = 0 ; i < proc ; i++) {
			HeapTuple tuple = tuptable->vals[i];

			//wids[i] = SPI_getvalue(tuple, tupdesc, 1);
			//nids[i] = SPI_getvalue(tuple, tupdesc, 2);
			//paths[i] = SPI_getvalue(tuple, tupdesc, 3);

			wid = atol(SPI_getvalue(tuple, tupdesc, 1));
			path = SPI_getvalue(tuple, tupdesc, 2);

			//to trim space from path string
			for (j = 1 ; j < 9 ; j++) {
				if (path[j] == ' ') {
					path[j] = '\0';
					break;
				}
			}

			if (fdd)  {
				fprintf(fdd, "way_id : %lu ; path : %s \n", wid, path);
				fflush(fdd);
			}

			if (prewid != -1 && wid != prewid && kh_size(hpath) > 0) {

				//allocate mem for (way_id, path) condition pair
				if (fdd) {
				 	fprintf(fdd, "the wn_paths allocated size %ld \n", kh_size(hpath) * 24 * sizeof(char));
					fflush(fdd);
				}

				wn_paths = palloc0(kh_size(hpath) * 24 * sizeof(char));
				kv_push(char *,indqstr, wn_paths);

				//for debug purpose
				for (j = kh_begin(hdnp) ; j != kh_end(hdnp) ; j++) {
					if (kh_exist(hdnp, j) && fdd) {
						fprintf(fdd, "The path with number : %d in khash(hdnp) is %s\n", j, kh_key(hdnp, j));
						fflush(fdd);
					}
				}

				//eliminate the duplicated nodes
				print_size = 0;
				for (j = kh_begin(hpath) ; j != kh_end(hpath) ; j++) {
					if (kh_exist(hpath, j)) {
						if (kh_get(dnp, hdnp, kh_key(hpath, j)) == kh_end(hdnp)) {
							 print_size += sprintf(wn_paths + print_size, "(%lu,\'%s\'),", prewid, kh_key(hpath, j));
						} else {
							if (fdd) {
								fprintf(fdd, "The duplicated way : %ld node path : %s \n", prewid, kh_key(hpath, j));
								fflush(fdd);
							}
						}
					}
				}

				if (fdd) {
					fprintf(fdd, "The way indirectly included nodes are : %s \n", wn_paths);
					fflush(fdd);
				}

				qstr_size += print_size;

				//deallocate the strings
				for (j = kh_begin(hpath) ; j != kh_end(hpath) ; j++) {
					if (kh_exist(hpath, j)) {
						//void * anc_path = kh_key(hpath, j);
						//pfree(anc_path);
					}
				}

				//clean up the previous way's nodes lists
				kh_clear(path, hpath);
				kh_clear(dnp, hdnp);
				way_count++;
			} 
			//get ancestors of the node and push to hashset
			get_ancestors(path, hpath);
			kh_put(dnp, hdnp, path, &rabs);
			prewid = wid;
		}

		//codes for the last way
		if (proc > 0 && kh_size(hpath) > 0) {
			if (prewid == -1) 
				prewid = wid;
			//allocate mem for (way_id, path) condition pair
			if (fdd) fprintf(fdd, "wn_paths allocated size %ld \n", kh_size(hpath) * 24 * sizeof(char));
			if (fdd) fflush(fdd);
			wn_paths = palloc0(kh_size(hpath) * 24 * sizeof(char));
			kv_push(char *,indqstr, wn_paths);

			//eliminate the duplicated nodes
			print_size = 0;
			for (j = kh_begin(hpath) ; j != kh_end(hpath) ; j++) {
				if (kh_exist(hpath, j)) {
					if (kh_get(dnp, hdnp, kh_key(hpath, j)) == kh_end(hdnp)) {
						 print_size += sprintf(wn_paths + print_size, "(%lu,\'%s\'),", prewid, kh_key(hpath, j));
					} else {
						if (fdd) {
							fprintf(fdd, "The duplicated way : %ld node path : %s \n", prewid, kh_key(hpath, j));
							fflush(fdd);
						}
					}
				}
			}

			qstr_size += print_size;
			way_count++;
		}

		// guard against the empty indirectly nodes
		if (kv_size(indqstr) == 0) {
			*inode_num = 0;
			*inodeids = NULL;
		} else {

			//catenate all the condition pairs
			if (fdd) fprintf(fdd, "wn_qstr allocated size %ld \n", (qstr_size + 256) * sizeof(char));
			if (fdd) fflush(fdd);
			wn_qstr = palloc0((qstr_size + 256) * sizeof(char));


			j = sprintf(wn_qstr, "select distinct node_id from way_trees where (way_id, path) in (");
			for (i = 0 ; i < kv_size(indqstr) ; i++) {
				j += sprintf(wn_qstr + j, "%s", kv_A(indqstr, i));
			}

			wn_qstr[j - 1] = ')';

			if (fdd) {
				fprintf(fdd, "The indirectly included nodes query string is : %s ; \n", wn_qstr); 
				fflush(fdd);
			}

			ret2 = SPI_execute(wn_qstr, true, 0);
			proc2 = SPI_processed;

			*inode_num = proc2 ;

			if (ret2 > 0 && SPI_tuptable != NULL) {
				TupleDesc tupdesc = SPI_tuptable->tupdesc;
				SPITupleTable * tuptable = SPI_tuptable;

				if (fdd) {
					fprintf(fdd, "the inodeids allocated size %ld\n", proc2*sizeof(long));
					fprintf(fdd, "the way number of vquery is %d\n", way_count);
					fflush(fdd);
				}
				*inodeids = palloc0(proc2 * sizeof(long));
				long * retids = *inodeids ;
				for (i = 0 ; i < proc2 ; i++) {
					HeapTuple tuple = tuptable->vals[i];

					retids[i] = atol(SPI_getvalue(tuple, tupdesc, 1));
				}

				//deallocate the memories
				for (i = 0 ; i < kv_size(indqstr) ; i++) {
					//void * wid_path_pair_str = kv_A(indqstr, i);
					//pfree(wid_path_pair_str);
				}

				for (i = kh_begin(hpath) ; i != kh_end(hpath) ; i++) {
					if (kh_exist(hpath, i)) {
						//void * p = kh_key(hpath, i);
						//pfree(p);
					}
				}

				kh_destroy(path, hpath);
				kh_destroy(dnp, hdnp);
				kv_destroy(indqstr);

			} else {
				// here query goes wrong
			}
		}

	} else {
		// here query goes wrong

	}

	if (fdd) fclose(fdd);

	return *inode_num;
}
