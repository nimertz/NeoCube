--ALTER SYSTEM SET jit=off;
--select pg_reload_conf();

-- Some useful (clustered) indexes on existing tables
-- Organize all children of nodes together
create index if not exists index_parentnode_id on nodes(parentnode_id);
alter table nodes cluster on "index_parentnode_id";
-- A covering index on object_id, tag_id. This way, both orders are covered.
-- Clustering should not be needed, as both indexes are covering
-- alter table objecttagrelations cluster on "PK_objecttagrelations";
create index if not exists index_object_tag_order on objecttagrelations(object_id, tag_id);
create index if not exists index_tag_object_order on objecttagrelations(tag_id, object_id);

-- Returns all nodes in the subtree from (parent node, hierarchy)
drop function if exists get_subtree_from_parent_node(int) cascade;
create function get_subtree_from_parent_node(int) 
returns setof nodes 
as '
WITH RECURSIVE allsubtags AS(
    select N.id, N.tag_id, N.hierarchy_id, N.parentnode_id
	from nodes N 
	where N.id = $1
    UNION ALL
    select N.id, N.tag_id, N.hierarchy_id, N.parentnode_id
	from nodes N
		join allsubtags A on A.id = N.parentnode_id
)  select * from allsubtags
' language SQL;

-- Returns all nodes in the level below (parent node, hierarchy)
drop function if exists get_level_from_parent_node(int, int) cascade;
create function get_level_from_parent_node(int, int) 
returns setof nodes 
as '
    select N.id, N.tag_id, N.hierarchy_id, N.parentnode_id
	from nodes N 
	where N.parentnode_id = $1 and N.hierarchy_id = $2
' language SQL;

-- Returns all nodes in the level of (sibling, hierarchy)
drop function if exists get_level_from_sibling(int, int) cascade;
create function get_level_from_sibling(int, int) 
returns setof nodes 
as '
    select N.id, N.tag_id, N.hierarchy_id, N.parentnode_id
	from nodes N 
		join nodes N1 on N.parentnode_id = N1.parentnode_id
	where N1.id = $1 and N1.hierarchy_id = $2
' language SQL;

-- Flattening hierarchies of nodes --> tags
drop materialized view if exists flattened_hierarchies cascade;
create materialized view flattened_hierarchies 
as
select N.parentnode_id, N.id as node_id, (get_subtree_from_parent_node(N.id)).tag_id
from nodes N;
create index index_flattened_hierarchies on flattened_hierarchies(parentnode_id, node_id, tag_id);

-- Full materialisation of parent_nodes --> nodes --> tags --> objects
drop materialized view if exists nodes_taggings cascade;
create materialized view nodes_taggings 
as
select H.parentnode_id, H.node_id, H.tag_id, O.object_id 
from flattened_hierarchies H
	join objecttagrelations O on O.tag_id = H.tag_id;
-- Index on parent_nodes --> objects --> nodes (for grouping by child nodes)
create index nodes_taggings_pid_oid_nid on nodes_taggings(parentnode_id, object_id, node_id);
-- Index on nodes --> objects (for retrieving cells)
create index nodes_taggings_nid_oid on nodes_taggings(node_id, object_id);

-- Full materialisation of tagsets --> tags --> objects
drop materialized view if exists tagsets_taggings cascade;
create materialized view tagsets_taggings
as
select T.tagset_id as tagset_id, R.tag_id, R.object_id
from tags T 
    join objecttagrelations R on R.tag_id = T.id;
-- Index on tagsets --> objects --> tags (tags matter here)
create index tagsets_taggings_sid_oid_tid on tagsets_taggings(tagset_id, object_id, tag_id);
-- Index on tags --> objects
-- Currently, queries are run against objecttagrelations which has the same information
-- create index tagsets_taggings_tid_oid on tagsets_taggings(tag_id, object_id);
