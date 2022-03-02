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

//find all images with dogs - 45 - id: 691
MATCH (root:Node)-[:NODE_HAS_TAG]-(t:Tag {name:"Dog"})
MATCH (root)<-[:HAS_PARENT*]-()-[:NODE_HAS_TAG]->(tag: Tag)<-[:TAGGED]-(o:Object)
RETURN o;

//find all images within entity hierarchy
MATCH (root: Node {id:40})
MATCH (root)<-[:HAS_PARENT*]-()-[:NODE_HAS_TAG]->()<-[:TAGGED]-(o:Object)
RETURN o;

//find all images from 2015
 MATCH (tag :Tag:Numerical {name:2015})<-[:TAGGED]-(o)
 return o;


// Combined state - ids unknown - 295 ms
MATCH (dogRoot: Node)-[:NODE_HAS_TAG]-(dogTag: Tag {name:"Dog"})
MATCH (dogRoot)<-[:HAS_PARENT*]-()-[:NODE_HAS_TAG]->()<-[:TAGGED]-(o: Object)
MATCH (ent : Node)-[:NODE_HAS_TAG]->(entTag: Tag {name: "Entity"})
WHERE EXISTS {
    MATCH (ent)<-[:HAS_PARENT*]-()-[:NODE_HAS_TAG]->()<-[:TAGGED]-(o)
} AND EXISTS {
    MATCH (tag:Tag:Numerical {name:2015})<-[:TAGGED]-(o)
}
RETURN o;

//Combined state - ids known - 400 ms
MATCH (root: Node {id:691})-[:NODE_HAS_TAG]-(t: Tag)
MATCH (root)<-[:HAS_PARENT*]-()-[:NODE_HAS_TAG]->()<-[:TAGGED]-(o: Object)
WHERE EXISTS {
    MATCH (ent: Node {id:40})<-[:HAS_PARENT*]-()-[:NODE_HAS_TAG]->()<-[:TAGGED]-(o)
} AND EXISTS {
    MATCH (tag :Tag:Numerical {id: 1350})<-[:TAGGED]-(o)
}
RETURN o;