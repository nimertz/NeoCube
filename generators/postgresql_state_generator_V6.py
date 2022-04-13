"""
@Author : Björn Thór Jónsson
This parser creates photocube postgresql state queries.
pip install postgresql
python3 postgresql_state_generator_V6.py
"""


import sys
import psycopg2
import time
import datetime

connection = psycopg2.connect(user="photocube",
                                  password="123",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="photocube")



def print_state(query):
    cursor = connection.cursor()
    cursor.execute(query)

    results = cursor.fetchall()
    print(len(results))
    for row in results:
        #print("idx:%i:idy:%i:idz:%i:cnt:%i:uri:%s" % (row[0], row[1], row[2], row[4], row[3]))
        pass

    cursor.close()

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
    print("Total Cells: %i" % (states))

    frontstr = "select X.idx, X.idy, X.idz, O.file_uri, X.cnt from (select "
    midstr = "from ("
    endstr = "group by "
    for i in range(numdims):
        frontstr = frontstr + ("R%i.id as %s, " % (i+1, attrs[i]))

        if (types[i] == "S"):
            midstr = midstr + ("select T.object_id, T.tag_id as id from tagsets_taggings T where T.tagset_id = %i) R%i " % (filts[i], i+1))
        elif (types[i] == "H"):
            midstr = midstr + ("select N.object_id, N.node_id as id from nodes_taggings N where N.parentnode_id = %i) R%i " % (filts[i], i+1))

        if (i == 0):
            midstr = midstr + "join ("
        elif (i == numtots -1):
            midstr = midstr + ("on R1.object_id = R%i.object_id " % (i+1))
        else:
            midstr = midstr + ("on R1.object_id = R%i.object_id join (" % (i+1))

        endstr = endstr + ("R%i.id" % (i+1))
        if (i < numdims-1):
            endstr = endstr + ", "
        print(types[i],filts[i])

    for i in range(numdims, numtots):
        if (types[i] == "S"):
            midstr = midstr + ("select T.object_id, T.tag_id as id from tagsets_taggings T where T.tagset_id = %i) R%i " % (filts[i], i+1))
        elif (types[i] == "H"):
            midstr = midstr + ("select N.object_id, N.node_id as id from nodes_taggings N where N.parentnode_id = %i) R%i " % (filts[i], i+1))
        elif (types[i] == "T"):
            midstr = midstr + ("select R.object_id from objecttagrelations R where R.tag_id = %i) R%i " % (filts[i], i+1))
        elif types[i] == "M":
            midstr = midstr + ("select R.object_id from objecttagrelations R where R.tag_id in ")
            for j in range(len(filts[i])):
                if j == 0:
                    midstr = midstr + ("(%i" % (filts[i][j]))
                else:
                    midstr = midstr + (", %i" % (filts[i][j]))
            midstr = midstr + (")) R%i " % (i+1))

        if (i == (numtots-1)):
            midstr = midstr + ("on R1.object_id = R%i.object_id " % (i+1))
        else:
            midstr = midstr + ("on R1.object_id = R%i.object_id join (" % (i+1))
        print(types[i],filts[i])

    for i in range(numdims, 3):
        frontstr = frontstr + ("1 as %s, " % attrs[i])

    frontstr = frontstr + ("max(R1.object_id) as object_id, count(distinct R1.object_id) as cnt ")
    endstr = endstr + (") X join cubeobjects O on X.object_id = O.id;")
    sqlstr = ("%s %s %s" % (frontstr, midstr, endstr))
    print(sqlstr)
    print_state(sqlstr)

numdims = 0
numtots = 0
readingdims = True
qs = 1
dims = []
types = []
filts = []
for L in sys.stdin:
    C = L.split()
    if C[0] == "G":
        start = datetime.datetime.now()
        for i in range(qs):
            get_state()
        end = datetime.datetime.now()
        duration = end - start
        print("C Time",((duration // datetime.timedelta(microseconds=1)) / qs) / 1000.0)
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

        cursor = connection.cursor()
        query = "select * from get_level_from_parent_node(%i, %i);" % (node, hier)
        print(query)
        cursor.execute(query)

        results = cursor.fetchall()
        for row in results:
            print("id =", row[0], " tag_id =", row[1], " hierarchy_id =", row[2], " parentnode_id =", row[3])
            dims[numtots].append(int(row[0]))
        cursor.close()

        numtots += 1
        if readingdims:
            numdims = numdims + 1
    #Tagset
    if C[0] == "S":
        tagset = int(C[1])
        dims.append([])
        types.append("S")
        filts.append(tagset)

        cursor = connection.cursor()
        query = "select * from tags where tagset_id = %i;" % (tagset)
        print(query)
        cursor.execute(query)

        results = cursor.fetchall()
        for row in results:
            #print("id =", row[0], " tagtype_id =", row[1], " tagset_id =", row[2])
            dims[numtots].append(int(row[0]))
        cursor.close()

        numtots += 1
        if readingdims:
            numdims = numdims + 1

    if C[0] == "T":
        dims.append([])
        types.append("T")
        dims[numtots].append(int(C[1]))
        filts.append(int(C[1]))

        numtots += 1
        if readingdims:
            numdims = numdims + 1

    if C[0] == "M":
        num = int(C[1])
        dims.append([])
        types.append("M")
        for i in range(num):
            dims[numtots].append(int(C[i+2]))
        filts.append(dims[numtots])

        numtots += 1
        if readingdims:
            numdims = numdims + 1

