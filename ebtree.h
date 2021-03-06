// Initialization of the khash related functions
	//
KHASH_SET_INIT_STR(str)
	//hashset in get_more_nodes() to hold the directly-included nodes' paths
KHASH_SET_INIT_STR(dnp)
KHASH_SET_INIT_STR(path)
KHASH_SET_INIT_INT64(nid)
KHASH_MAP_INIT_STR(tlstr, int)
//KHASH_MAP_INIT_STR(mami, long)

typedef struct tt {
	char tile[9] ;
	int count;
	//struct tt * children[4] ;
	unsigned long geobits;
	int level;
	float eub;
	float elb;
	//bool within;
} Tile_tree;

typedef struct qt {
	char tile[9] ;
	int count;
	struct qt * children[4] ;
	unsigned long geohash_bits;
	float lbound;
	float ubound; 
} qtree;

typedef struct pn { 
	long nid;
	float error;
	char * tile;
} presort_node;


/* internal functions that only called by functions in this file */
float seg2pt(double x1, double y1, double x2, double y2, double x3, double y3) ;
float pt2pt(double x1, double y1, double x2, double y2) ;
int gcode(double lon, double lat, float err, char* tile) ;
int lc(char * c, char * p) ;
int rc(char * c, char * p) ;
float simpl(long nids[], int lons[], int lats[], int i, int j, char path[], float p, float errors[], char* paths[], int sizes[], char* tiles[]) ;
int arborway(long cur_way_id, long ids[], float errors[], char* paths[], int sizes[], char* tiles[], int num) ;
int gcode2(long xy_bits, int bits_count, char* tile);

int get_ancestors(char * npath, khash_t(path) * hpath) ;
int window2tiles(double wx1, double wy1, double wx2, double wy2, khash_t(str) * h, char *tilestr) ;
ArrayType * iinids2array(khash_t(nid) * hnid, long *nids, int nidlen) ;

int dump2file(qtree * qn, FILE * fdd, int level) ;
int compare_tt_keys(void *key1, void *key2) ;
Tile_tree* read_tile(char* tile, int level, Tile_tree * ttp) ;
int covered_nodes_number(Tile_tree * ttp, double wx1, double wy1, double wx2, double wy2, bool *isOverlapped) ;

heap* dig_down(heap* h, Tile_tree * ttp) ;
unsigned long tile2gbits(char * tile, int level) ;
char* gbits2tile(unsigned long geobits, int level_depth, char geocode[]) ;
unsigned long xy2gbits(double x, double y, int level) ;

int depth(double wx1, double wy1, double wx2, double wy2) ;
char * * qtile(int level, double wx1, double wy1, double wx2, double wy2, char ** qtiles) ;

//functions for vquery processing
int get_nodes(double wx1, double wy1, double wx2, double wy2, int level, presort_node **pns, int remain_len, int * nodes_num, int *mami_num, float * mami_err) ;
int set_result(long * rst, int rst_top, int nlimit, presort_node ** pns, int level, int * tln_num, int *topmm, float *mamis) ;
int get_more_nodes(long * rst, int rst_num, long **inodeids, int * inode_num) ;
