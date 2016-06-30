#include "postgres.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(gcode_c);

Datum gcode_c(PG_FUNCTION_ARGS) {
	text * retval ;
	float8 lon, lat;
	float4 err;

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

	lon = PG_GETARG_FLOAT8(0);
	lat = PG_GETARG_FLOAT8(1);
	err = PG_GETARG_FLOAT4(2);

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

		memset((void *)geocode, 0, 8);
		mod_num = (level_depth * 2) % 5;
		div_num = (level_depth * 2) / 5;
		for (i = 0; i < div_num ; i++) {
			single_code = (geobits >> ((div_num - 1) * 5 + mod_num)) & 0x1f;
			geocode[i] = codes[single_code];
			div_num--;
		}
		
		single_code = (geobits << (5 - mod_num)) & 0x1f;
		geocode[div_num] = codes[single_code];
	}

	retval = (text *) palloc0(8 + VARHDRSZ); 
	SET_VARSIZE(retval, 8 + VARHDRSZ);
	memcpy((void *)VARDATA(retval), (void *)geocode, 8);
	PG_RETURN_TEXT_P(retval);
}
