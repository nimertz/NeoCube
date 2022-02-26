//cubeobjects
MATCH (co : Object ) return co;
MATCH (co : Object {id: 1}) return co;

//Hierarchy
MATCH (co : Hierarychy ) return co;
MATCH (co : Hierarychy {id: 1}) return co;

//​ Node​/{id}​/tree - Find everything under node with id - based on https://stackoverflow.com/questions/28557055/neo4j-deep-hierarchy-query
// MATCH (root: Node {id: 40})
// MATCH p = (root)<-[:HAS_PARENT*]-() //define path
// WITH last(nodes(p)) AS currNode, length(p) AS depth, root
// OPTIONAL MATCH (currNode)<-[r:HAS_PARENT]-(children: Node) // tranverse children - optional as it can be 0
// MATCH (currNode)-[:NODE_HAS_TAG]->(tag: Tag)-[:IN_TAGSET]->(ts: Tagset)
// RETURN currNode, root, depth, tag, ts.name as tagset;



//node hierarchy JSON LIKE FORMAT
MATCH p=(root:Node {id:40})<-[:HAS_PARENT*]-()-[:NODE_HAS_TAG]->(tag: Tag)-[:IN_TAGSET]->(ts: Tagset)
WITH collect(p) AS ps
CALL apoc.convert.toTree(ps) yield value
RETURN value;

MATCH p=(root:Node {id:5})<-[:HAS_PARENT*]-()-[:NODE_HAS_TAG]->(tag: Tag)
WITH collect(p) AS ps
return ps;

//find immediate node children
MATCH (root: Node {id: 5})
MATCH p = (root)<-[:HAS_PARENT]-()
RETURN p;

// TODO state
MATCH p=(o: Object)-[:TAGGED]->(t: Tag)
WHERE EXISTS {
    MATCH (root:Node {id: 3})<-[:HAS_PARENT*]-()
    WHERE EXISTS {
        MATCH (otherRoot:Node)<-[:HAS_PARENT*]-()-[:NODE_HAS_TAG]->(tag: Tag {name: "Dog"})
    }
} AND t.name > date({year: 2015})
RETURN p limit 50;