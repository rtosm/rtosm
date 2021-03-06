# rtosm

A [Postgresql](https://github.com/postgres/postgres) database extension to provide the functionality of real-time simplification of spatial objects in Openstreetmap API Database which powers [openstreetmap-website](https://github.com/openstreetmap/openstreetmap-website).

The openstreetmap API database currently support retrieving spatial data throuth the web inferface of map api v0.6. However, the limitation of the data retrieve is as follows: only data requests with the requested area is no more than 0.25 square degree can be processed. There is such a limitation because data request with a large requested area result in a scenario that all the spatial objects in that area will be read from the database and send back as response. The size of the response will be HUGE and the query process will badly corrupt the performance of the database. Things are perhaps different if the spatial objects can be filtered and simplified in the database before they are packed into a data response. the above thoughts are the motivation of this project: remove the limitations on the openstreetmap API database to make it more flexible for online visualization and online creation.

The core component of RTOSM is the EB-tree(Error Bounds Tree) data structure. The EB-tree is a self-balanced binary tree and is a hierarchical representation of an objects sequence. Each node of EB-tree contains two kinds of information about the object in objects sequence, one is about the sequence number, the other is about the error inccured by omitting the object. 
By building the EB-tree for each of the object sequence such as ways (nodes sequence) and relations(ways sequence), The objects can be filtered in aware of error bounds of the resulted data. The construction, querying, updating of EB-tree are implemented in PL/pgSQL for simplicity.

This extension uses following two libraries:


1. [klib](https://github.com/attractivechaos/klib)

2. [c-minheap-array](https://github.com/armon/c-minheap-array)

# License

This software is licensed under the [GNU General Public License 2.0](http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt),
a copy of which can be found in the [LICENSE](LICENSE) file.

# Installation

The prerequisite of the rtosm is openstreetmap-website which is the backbone code of the openstreetmap API database.

## openstreetmap-website

To install rtosm, you need to install the [openstreetmap-website](https://github.com/openstreetmap/openstreetmap-website) first.

## rtosm install
```
make
sudo make install

psql -d openstreetmap
create extension intarray;
create extension rtosm;
select build_way_trees();
select build_relation_trees();
select build_indexes();
select tileid_c(30000);
```
# Usage

Following query will select ids of all the nodes the simplified spatial objects need. No matter how large the bbox(x1, y1, x2, y2) size is, the output size of the vquery_c will be reasonable, the k is the limit of node number and the e is the error limit. with the simplified objects' node, you can do analysis or visualization. 
```
select unnest(vquery_c(bbox.x1, bbox.y1, bbox.x2, bbox.y2, k, e));
```

