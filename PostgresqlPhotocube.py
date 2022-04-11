
class PostgresqlPhotocube:
    def __init__(self,conn):
        self.conn = conn
        self.cursor = self.conn.cursor()


    def disconnect(self):
        self.conn.close()

    @staticmethod
    def get_tag_by_id(cursor, tag_id):
        cursor.execute("SELECT * FROM tags WHERE id = %s", (tag_id,))
        return cursor.fetchone()

    @staticmethod
    def get_tags_in_tagset(cursor, tagset_id):
        cursor.execute("select * from tags where tagset_id = %i;" % (tagset_id))
        return cursor.fetchall()

    @staticmethod
    def get_level_from_parent_node(cursor, node_id, hierarchy_id):
        cursor.execute("select * from get_level_from_parent_node(%i, %i);" % (node_id, hierarchy_id))
        return cursor.fetchall()

    @staticmethod
    def gen_state_query(numdims, numtots, types, filts):
        attrs = ["idx", "idy", "idz"]

        frontstr = "select X.idx, X.idy, X.idz, O.file_uri, X.cnt from (select "
        midstr = "from ("
        endstr = "group by "
        for i in range(numdims):
            frontstr = frontstr + ("R%i.id as %s, " % (i + 1, attrs[i]))

            if types[i] == "S":
                midstr = midstr + (
                            "select T.object_id, T.tag_id as id from tagsets_taggings T where T.tagset_id = %i) R%i " % (
                    filts[i], i + 1))
            elif types[i] == "H":
                midstr = midstr + (
                            "select N.object_id, N.node_id as id from nodes_taggings N where N.parentnode_id = %i) R%i " % (
                    filts[i], i + 1))

            if i == 0:
                midstr = midstr + "join ("
            elif i == numtots - 1:
                midstr = midstr + ("on R1.object_id = R%i.object_id " % (i + 1))
            else:
                midstr = midstr + ("on R1.object_id = R%i.object_id join (" % (i + 1))

            endstr = endstr + ("R%i.id" % (i + 1))
            if i < numdims - 1:
                endstr = endstr + ", "
            #print(types[i], filts[i])

        for i in range(numdims, numtots):
            if types[i] == "S":
                midstr = midstr + (
                            "select T.object_id, T.tag_id as id from tagsets_taggings T where T.tagset_id = %i) R%i " % (
                    filts[i], i + 1))
            elif types[i] == "H":
                midstr = midstr + (
                            "select N.object_id, N.node_id as id from nodes_taggings N where N.parentnode_id = %i) R%i " % (
                    filts[i], i + 1))
            elif types[i] == "T":
                midstr = midstr + ("select R.object_id from objecttagrelations R where R.tag_id = %i) R%i " % (
                filts[i], i + 1))
            elif types[i] == "M":
                midstr = midstr + ("select R.object_id from objecttagrelations R where R.tag_id in ")
                for j in range(len(filts[i])):
                    if j == 0:
                        midstr = midstr + ("(%i" % (filts[i][j]))
                    else:
                        midstr = midstr + (", %i" % (filts[i][j]))
                midstr = midstr + (")) R%i " % (i + 1))

            if i == (numtots - 1):
                midstr = midstr + ("on R1.object_id = R%i.object_id " % (i + 1))
            else:
                midstr = midstr + ("on R1.object_id = R%i.object_id join (" % (i + 1))
            #print(types[i], filts[i])

        for i in range(numdims, 3):
            frontstr = frontstr + ("1 as %s, " % attrs[i])

        frontstr = frontstr + ("max(R1.object_id) as object_id, count(distinct R1.object_id) as cnt ")
        endstr = endstr + (") X join cubeobjects O on X.object_id = O.id;")
        sqlstr = ("%s %s %s" % (frontstr, midstr, endstr))

        return sqlstr

    @staticmethod
    def execute_query(cursor,sqlstr):
        cursor.execute(sqlstr)
        return cursor.fetchall()