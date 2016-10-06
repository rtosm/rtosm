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
		//get the nodes which are ancestor of a selected way's directly included nodes;
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

PG_FUNCTION_INFO_V1(vquery_c);
/*
	user specify the query condition including the querying window and error tolerance and count limit
*/
Datum vquery_c(PG_FUNCTION_ARGS) { 
	int ncount = 0;
	/*
	double wx1 = PG_GETARG_FLOAT8(0);
	double wy1 = PG_GETARG_FLOAT8(1);
	double wx2 = PG_GETARG_FLOAT8(2);
	double wy2 = PG_GETARG_FLOAT8(3);
	int nlimit = PG_GETARG_INT32(4);
	float elimit = PG_GETARG_FLOAT4(5);


	if (nlimit <= 0) {
	}

	if (elimit <= 0) {
	}

	char * query_str = "select from  ";
	*/

	PG_RETURN_INT32(ncount);
}
