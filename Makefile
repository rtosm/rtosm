PG_CPPFLAGS = -Wall -std=gnu9x -O2 -Wno-declaration-after-statement
MODULE_big	=	rtosm_comp
EXTENSION		=	rtosm
#DATA_built=rtosm_c/comp_geo.sql
DATA				=	rtosm--0.1.sql
SRCS 				= comp_geo.c heap.c ebtree.c
OBJS 				= $(SRCS:.c=.o)

#DATA=geom_computation.sql node.sql prerequisite.sql query.sql relation.sql tree_operation.sql way.sql way_editing.sql
PG_CONFIG=pg_config
PGXS:=$(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
