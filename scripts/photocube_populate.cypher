//Clear data
//MATCH ()-[r]->() DELETE r;

//MATCH (n)
//DETACH DELETE n;


//MERGE constraints and indexes - unique constraint creates index
CREATE CONSTRAINT objects IF NOT EXISTS FOR (o:Object) REQUIRE o.id IS UNIQUE;
CREATE CONSTRAINT tags IF NOT EXISTS FOR (t:Tag) REQUIRE t.id IS UNIQUE;
CREATE CONSTRAINT tagsets IF NOT EXISTS FOR (ts:Tagset) REQUIRE ts.id IS UNIQUE;
CREATE CONSTRAINT hierarchies IF NOT EXISTS FOR (h:Hierarchy) REQUIRE h.id IS UNIQUE;
CREATE CONSTRAINT nodes IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE;

// for searching tags by name
CREATE INDEX tagname IF NOT EXISTS FOR (t:Tag) ON t.name;

//Load photocube data
USING PERIODIC COMMIT 500

//Objects
LOAD CSV WITH HEADERS FROM 'file:///cubeobjects.csv' AS co
MERGE (o:Object {id:toInteger(co.id), file_uri: co.file_uri, file_type: toInteger(co.file_type), thumbnail_uri: co.thumbnail_uri})
RETURN count(o);

//Tags
//Alphanumerical tags
LOAD CSV WITH HEADERS FROM 'file:///alphanumerical_tags.csv' AS at
MERGE (alpha:Tag:Alphanumerical {id:toInteger(at.id), name: at.name})
RETURN count(alpha);
//Numerical tags
LOAD CSV WITH HEADERS FROM 'file:///numerical_tags.csv' AS nt
MERGE (num:Tag:Numerical {id:toInteger(nt.id), name: toInteger(nt.name)}) //change to  float when appropriate
RETURN count(num);
//date tags
LOAD CSV WITH HEADERS FROM 'file:///date_tags.csv' AS dt
MERGE (date:Tag:Date {id:toInteger(dt.id), name:date(dt.name)})
RETURN count(date);
//time tags
LOAD CSV WITH HEADERS FROM 'file:///time_tags.csv' AS tt
MERGE (time:Tag:Time {id:toInteger(tt.id), name:time(tt.name)})
RETURN count(time);
//timestamp tags
USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM 'file:///timestamp_tags.csv' AS tst
MERGE (timestamp:Tag:Timestamp {id:toInteger(tst.id), name:apoc.date.parse(tst.name, "s", "yyyy-MM-dd HH:mm:ss")}) // to seconds
RETURN count(timestamp);

//Object --> Tag  
USING PERIODIC COMMIT 500 // Needed for large data sets
LOAD CSV WITH HEADERS FROM 'file:///objecttagrelations.csv' AS ot
MATCH (o:Object {id:toInteger(ot.object_id)}), (t:Tag {id:toInteger(ot.tag_id)})
MERGE (o)-[r:TAGGED]->(t)
RETURN count(r);

//Tagsets
LOAD CSV WITH HEADERS FROM 'file:///tagsets.csv' AS ts
MERGE (tset:Tagset {id:toInteger(ts.id), name: ts.name})
RETURN count(tset);

//Node
LOAD CSV WITH HEADERS FROM 'file:///nodes.csv' AS node
MERGE (n:Node {id:toInteger(node.id)})
RETURN count(n);

//Hierarchies
LOAD CSV WITH HEADERS FROM 'file:///hierarchies.csv' AS h
MERGE (hierarchy:Hierarchy {id:toInteger(h.id), name: h.name})
RETURN count(hierarchy);

//Node --> Hierarchy
LOAD CSV WITH HEADERS FROM 'file:///nodes.csv' AS node
MATCH (n:Node {id:toInteger(node.id)}), (h:Hierarchy {id:toInteger(node.hierarchy_id)})
MERGE (n)-[r:IN_HIERARCHY]->(h)
RETURN count(r);

//Node --> Tag
LOAD CSV WITH HEADERS FROM 'file:///nodes.csv' AS node
MATCH (n:Node {id:toInteger(node.id)}), (t:Tag {id:toInteger(node.tag_id)})
MERGE (n)-[r:REPRESENTS]->(t)
RETURN count(r);

//Hierarchy --> root node
LOAD CSV WITH HEADERS FROM 'file:///hierarchies.csv' AS h
MATCH (hierarchy:Hierarchy {id:toInteger(h.id)}), (node:Node {id:toInteger(h.rootnode_id)})
MERGE (hierarchy)-[r:HAS_ROOT]->(node)
RETURN count(r);

//Hierarchy --> tagset
LOAD CSV WITH HEADERS FROM 'file:///hierarchies.csv' AS h
MATCH (hierarchy:Hierarchy {id:toInteger(h.id)}), (tset:Tagset {id:toInteger(h.tagset_id)})
MERGE (hierarchy)-[r:HAS_TAGSET]->(tset)
RETURN count(r);

//node --> parent node
LOAD CSV WITH HEADERS FROM 'file:///nodes.csv' AS node
MATCH (n:Node {id:toInteger(node.id)}), (p:Node {id:toInteger(node.parentnode_id)})
MERGE (n)-[r:HAS_PARENT]->(p)
RETURN count(r);

//Tagset --> Tag | Tag --> Tagset
USING PERIODIC COMMIT 500 // Needed for large data sets
LOAD CSV WITH HEADERS FROM 'file:///tags.csv' AS t
MATCH (tag:Tag {id:toInteger(t.id)}), (tset:Tagset {id:toInteger(t.tagset_id)})
MERGE (tag)-[r:IN_TAGSET]->(tset)
MERGE (tset)-[l:HAS_TAG]->(tag)
RETURN count(r) + count(l);



