MODULES=rtosm_c/comp_geo
EXTENSION=rtosm
#DATA_built=rtosm_c/comp_geo.sql
DATA=rtosm--0.1.sql
#DATA=geom_computation.sql node.sql prerequisite.sql query.sql relation.sql tree_operation.sql way.sql way_editing.sql
PG_CONFIG=pg_config
PGXS:=$(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
