"""
@Author : Nikolaj Mertz
This parser creates photocube neo4j state & cell queries.
pip install neo4j
python3 neo4j_state_generator_V1.py
"""

import datetime
import sys

# connect to neo4j database
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123"))

def run_state_query(query):
    results = session.read_transaction(query_state, query)
    if(len(results) <= 10):
        for row in results:
            print("idx:%i:idy:%i:idz:%i:cnt:%i:uri:%s" % (row['idx'], row['idy'], row['idz'], row[4], row[3]))
            #pass
    return results

attrs = ["idx", "idy", "idz"]
def get_state():
    print("Dim 1: %i cells" % (len(dims[0])))
    states = len(dims[0])
    if numdims > 1:
        print("Dim 2: %i cells" % (len(dims[1])))
        states *= len(dims[1])
    if numdims > 2:
        print("Dim 3: %i cells" % (len(dims[2])))
        states *= len(dims[2])
    print("Total Cells: %i" % states)

    frontstr = "" # add profile / explain here if needed
    midstr = ""
    endstr = "RETURN "
    # handle empty dimensions
    for i in range(numdims, 3):
        endstr += ("1 as %s, " % attrs[i])
    # apply dimensions
    endstr, midstr = apply_dimensions(endstr, midstr)

    # apply rest of filters
    midstr = apply_filters(midstr)

    endstr += "max(o).file_uri as file_uri, count(o) as cnt;"

    sqlstr = ("\n%s %s %s\n" % (frontstr, midstr, endstr))
    print(sqlstr)
    return run_state_query(sqlstr)


def apply_filters(midstr):
    for i in range(numdims, numtots):
        if types[i] == "S":
            midstr += "MATCH (fil%i_ts: Tagset {id: %i}) " % (i + 1, filts[i])
            midstr += "MATCH (fil%i_ts)<-[:IN_TAGSET]-(R%i: Tag)<-[:TAGGED]-(o: Object)\n" % (i + 1, i + 1)
        elif types[i] == "H":
            midstr += "MATCH (fil%i_n: Node {id: %i})\n" % (i + 1, filts[i])
            midstr += "MATCH (fil%i_n)<-[:HAS_PARENT*]-(R%i : Node)-[:REPRESENTS]->(:Tag)<-[:TAGGED]-(o: Object)\n" % (
            i + 1, i + 1)
        elif types[i] == "T":
            midstr += "MATCH (fil%i_t: Tag {id: %i})\n" % (i + 1, filts[i])
            midstr += "MATCH (fil%i_t)<-[:TAGGED]-(o: Object)\n" % (i + 1)
        elif types[i] == "M":
            midstr += "MATCH (m_fil%i_t: Tag) WHERE m_fil%i_t.id IN [" % (i + 1, i + 1)
            for j in range(len(filts[i])):
                if j == 0:
                    midstr += "%i" % (filts[i][j])
                else:
                    midstr += ",%i" % (filts[i][j])
            midstr += "]\n"
            midstr += "MATCH (m_fil%i_t)<-[:TAGGED]-(o: Object)\n" % (i + 1)
        print(types[i], filts[i])
    return midstr


def apply_dimensions(endstr, midstr):
    for i in range(numdims):
        endstr += ("R%i.id as %s, " % (i + 1, attrs[i]))

        if types[i] == "S":
            midstr += "MATCH (dim%i_ts: Tagset {id: %i})\n" % (i + 1, filts[i])
            midstr += "MATCH (dim%i_ts)<-[:IN_TAGSET]-(R%i: Tag)<-[:TAGGED]-(o: Object)\n" % (i + 1, i + 1)
        elif types[i] == "H":
            midstr += "MATCH (dim%i_n: Node {id: %i})\n" % (i + 1, filts[i])
            midstr += "MATCH (dim%i_n)<-[:HAS_PARENT*]-(R%i : Node)-[:REPRESENTS]->(:Tag)<-[:TAGGED]-(o: Object)\n" % (
            i + 1, i + 1)
        print(types[i], filts[i])
    return endstr, midstr


def query_state(tx, query):
    result = tx.run(query)
    return list(result)

def get_object_under_node(tx,node):
    object_count_query = "MATCH (root: Node {id: %i })<-[:HAS_PARENT*]-()-[:REPRESENTS]->()<-[:TAGGED]-(o:Object) RETURN count(distinct o) as cnt;" % (node)
    result = tx.run(object_count_query)
    return list(result)

def get_objects_with_tag(tx,tags):
    objects_query = "MATCH (o:Object)-[:TAGGED]->(t:Tag) WHERE t.id IN %s RETURN count(distinct o) as cnt;" % (tags)
    result = tx.run(objects_query)
    return list(result)

def get_tag_by_id(tx, tag_id):
    result = tx.run("MATCH (t:Tag {id: $tag_id}) RETURN t.name as name, labels(t)", tag_id=tag_id)
    return list(result)

def get_tags_in_tagset(tx,tagset_id):
    result = tx.run("MATCH (t:Tag)-[r:IN_TAGSET]->(ts:Tagset {id: $tagset_id}) RETURN t.id,t.name, labels(t), ts.id", tagset_id=tagset_id)
    return list(result)

def get_level_from_parent_node(tx,node_id,hierarchy_id):
    result = tx.run("MATCH (root:Node {id: $node_id})<-[:HAS_PARENT]-(n : Node)-[:IN_HIERARCHY]->(h:Hierarchy {id: $hierarchy_id}) "
                    "MATCH (n)-[:REPRESENTS]->(t:Tag) "
                    "RETURN n.id, t.id, h.id, root.id", node_id=node_id, hierarchy_id=hierarchy_id)
    return list(result)

numdims = 0
numtots = 0
readingdims = True
qs = 1
dims = []
types = []
filts = []
with driver.session() as session:
    for L in sys.stdin:
        C = L.split()
        if C[0] == "G":
            start = datetime.datetime.now()
            for i in range(qs):
                res = get_state()
            end = datetime.datetime.now()
            duration = end - start
            print("C Time",((duration // datetime.timedelta(microseconds=1)) / qs) / 1000.0)

            print("Row returned: %i" % len(res))
            object_sum = 0
            x,y,z,smallest_cnt =1,1,1,1000000
            for row in res:
                if row[4] < smallest_cnt:
                    x,y,z,smallest_cnt = row['idx'], row['idy'], row['idz'],row[4]
                object_sum += row[4]
                #print("idx:%i:idy:%i:idz:%i:cnt:%i:uri:%s" % (row[0], row[1], row[2], row[4], row[3]))
                pass
            print("Total Object count: %i" % (object_sum))
            print("Smallest object count: x: %i, y: %i, z: %i - %i" % (x,y,z,smallest_cnt))


            numdims = 0
            numtots = 0
            readingdims = True
            dims = []
            types = []
            filts = []

        if C[0] == "F":
            readingdims = False
        #Hierarchy
        if C[0] == 'H':
            node = int(C[1])
            hier = int(C[2])
            dims.append([])
            types.append("H")
            filts.append(node)

            sublevel = session.read_transaction(get_level_from_parent_node, node, hier)

            for row in sublevel:
                print("id =", row[0], " tag_id =", row[1], " hierarchy_id =", row[2], " parentnode_id =", row[3])
                dims[numtots].append(int(row[0]))

            # count objects for filter
            objects = session.read_transaction(get_object_under_node, node)
            print("Objects for hierarchy H %i: %i" % (node,objects[0]['cnt']))

            numtots += 1
            if readingdims:
                numdims = numdims + 1
        #Tagset
        if C[0] == "S":
            tagset = int(C[1])
            dims.append([])
            types.append("S")
            filts.append(tagset)

            tagset_tags = session.read_transaction(get_tags_in_tagset, tagset)
            #print(results)

            # count objects for filter
            tags = []
            for row in tagset_tags:
                print("id =", row[0], "tagname =", row[1], " tagtype_id =", row[2], " tagset_id =", row[3])
                tags.append(row[0])
                dims[numtots].append(int(row[0]))
            
            objects_query = "MATCH (o:Object)-[:TAGGED]->(t:Tag) WHERE t.id IN %s RETURN count(distinct o) as cnt;" % (tagset_tags)
            objects = session.read_transaction(get_objects_with_tag, tags)
            print("Objects for Tagset S %i: %i" % (tagset,objects[0]['cnt']))

            numtots += 1
            if readingdims:
                numdims = numdims + 1

        if C[0] == "T":
            dims.append([])
            types.append("T")
            dims[numtots].append(int(C[1]))
            filts.append(int(C[1]))

            results = session.read_transaction(get_tag_by_id, int(C[1]))
            for row in results:
                print("tagname =", row[0], " type =", row[1][1])
                #pass

            numtots += 1
            if readingdims:
                numdims = numdims + 1

        if C[0] == "M":
            num = int(C[1])
            dims.append([])
            types.append("M")
            for i in range(num):
                dims[numtots].append(int(C[i+2]))
                
                results = session.read_transaction(get_tag_by_id, int(C[i+2]))
                for row in results:
                    print("tagname =", row[0], " type =", row[1][1])
                    #pass

            filts.append(dims[numtots])

            numtots += 1
            if readingdims:
                numdims = numdims + 1