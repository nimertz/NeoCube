from M3DatabaseInterface import M3DB


class PostgresqlPC(M3DB):
    """
    This class is used to connect to the PostgreSQL database and execute queries.
    """

    def __init__(self, conn):
        self.conn = conn

    def close(self):
        self.conn.close()

    def get_name(self):
        return "PostgreSQL"

    @staticmethod
    def __get_tag_by_id(cursor, tag_id):
        cursor.execute("SELECT * FROM tags WHERE id = %s", (tag_id,))
        return cursor.fetchone()

    def get_tag_by_id(self, tag_id):
        with self.conn.cursor() as cursor:
            return self.__get_tag_by_id(cursor, tag_id)

    @staticmethod
    def __get_tags_in_tagset(cursor, tagset_id):
        cursor.execute("select * from tags where tagset_id = %i;" % (tagset_id))
        return cursor.fetchall()

    def get_tags_in_tagset(self, tagset_id):
        with self.conn.cursor() as cursor:
            return self.__get_tags_in_tagset(cursor, tagset_id)

    @staticmethod
    def __get_level_from_parent_node(cursor, node_id, hierarchy_id):
        cursor.execute("select * from get_level_from_parent_node(%i, %i);" % (node_id, hierarchy_id))
        return cursor.fetchall()

    def get_level_from_parent_node(self, node_id, hierarchy_id):
        with self.conn.cursor() as cursor:
            return self.__get_level_from_parent_node(cursor, node_id, hierarchy_id)

    @staticmethod
    def gen_state_query(numdims, numtots, types, filts, baseline=False):
        attrs = ["idx", "idy", "idz"]

        frontstr = "select X.idx, X.idy, X.idz, O.file_uri, X.cnt from (select "
        midstr = "from ("
        endstr = "group by "

        endstr, frontstr, midstr = PostgresqlPC.__apply_dimensions(attrs, baseline, endstr, filts, frontstr, midstr,
                                                                   numdims, numtots, types)

        midstr = PostgresqlPC.__apply_filters(baseline, filts, midstr, numdims, numtots, types)

        for i in range(numdims, 3):
            frontstr = frontstr + ("1 as %s, " % attrs[i])

        frontstr = frontstr + ("max(R1.object_id) as object_id, count(distinct R1.object_id) as cnt ")
        endstr = endstr + (") X join cubeobjects O on X.object_id = O.id;")
        sqlstr = ("%s %s %s" % (frontstr, midstr, endstr))

        return sqlstr

    @staticmethod
    def gen_cell_query(numdims, numtots, types, filts):
        if numtots == 0:
            return "select O.id as Id, O.file_uri as fileURI from cubeobjects O;"

        frontstr = "select distinct O.id as Id, O.file_uri as fileURI, TS.name as T from (select R1.object_id "
        midstr = " from ("
        endstr = ") X join cubeobjects O on X.object_id = O.id join objecttagrelations R2 on O.id = R2.object_id join timestamp_tags TS on R2.tag_id = TS.id order by TS.name;"

        for i in range(numdims):
            if types[i] == "T":
                midstr = midstr + (
                        " select R.object_id from objecttagrelations R where R.tag_id = %i) R%i " % (
                    filts[i], i + 1))
            elif types[i] == "H":
                midstr = midstr + (
                        " select N.object_id from nodes_taggings N where N.node_id = %i) R%i " % (
                    filts[i], i + 1))

            if i == 0:
                midstr = midstr + "join ("
            elif i == numtots - 1:
                midstr = midstr + ("on R1.object_id = R%i.object_id " % (i + 1))
            else:
                midstr = midstr + ("on R1.object_id = R%i.object_id join (" % (i + 1))

        for i in range(numdims, numtots):
            if types[i] == "S":
                midstr = midstr + (
                        " select T.object_id from tagsets_taggings T where T.tagset_id = %i) R%i " % (
                    filts[i], i + 1))
            elif types[i] == "H":
                midstr = midstr + (
                        " select N.object_id from nodes_taggings N where N.node_id = %i) R%i " % (
                    filts[i], i + 1))
            elif types[i] == "T":
                midstr = midstr + (" select R.object_id from objecttagrelations R where R.tag_id = %i) R%i " % (
                    filts[i], i + 1))
            elif types[i] == "M":
                midstr = midstr + (" select R.object_id from objecttagrelations R where R.tag_id in ")
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

        cell_query = ("%s %s %s" % (frontstr, midstr, endstr))
        return cell_query

    @staticmethod
    def __apply_filters(baseline, filts, midstr, numdims, numtots, types):
        for i in range(numdims, numtots):
            if types[i] == "S":
                if baseline:
                    midstr = midstr + (
                            "select T.object_id, T.tag_id as id from (SELECT t.tagset_id, r.tag_id, r.object_id FROM tags t JOIN objecttagrelations r ON r.tag_id = t.id) T where T.tagset_id = %i) R%i " % (
                        filts[i], i + 1))
                else:
                    midstr = midstr + (
                            "select T.object_id, T.tag_id as id from tagsets_taggings T where T.tagset_id = %i) R%i " % (
                        filts[i], i + 1))
            elif types[i] == "H":
                if baseline:
                    midstr = midstr + (
                            "select N.object_id, N.node_id as id from (SELECT h.parentnode_id, h.node_id, h.tag_id, o.object_id FROM (SELECT n.parentnode_id, n.id AS node_id, (get_subtree_from_parent_node(n.id)).tag_id AS tag_id FROM nodes n) h JOIN objecttagrelations o ON o.tag_id = h.tag_id) N where N.node_id = %i) R%i " % (
                        filts[i], i + 1))
                else:
                    midstr = midstr + (
                            "select N.object_id, N.node_id as id from nodes_taggings N where N.node_id = %i) R%i " % (
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
            # print(types[i], filts[i])
        return midstr

    @staticmethod
    def __apply_dimensions(attrs, baseline, endstr, filts, frontstr, midstr, numdims, numtots, types):
        for i in range(numdims):
            frontstr = frontstr + ("R%i.id as %s, " % (i + 1, attrs[i]))

            if types[i] == "S":
                if baseline:
                    midstr = midstr + (
                            "select T.object_id, T.tag_id as id from (SELECT t.tagset_id, r.tag_id, r.object_id FROM tags t JOIN objecttagrelations r ON r.tag_id = t.id) T where T.tagset_id = %i) R%i " % (
                        filts[i], i + 1))
                else:
                    midstr = midstr + (
                            "select T.object_id, T.tag_id as id from tagsets_taggings T where T.tagset_id = %i) R%i " % (
                        filts[i], i + 1))
            elif types[i] == "H":
                if baseline:
                    midstr = midstr + (
                            "select N.object_id, N.node_id as id from (SELECT h.parentnode_id, h.node_id, h.tag_id, o.object_id FROM (SELECT n.parentnode_id, n.id AS node_id, (get_subtree_from_parent_node(n.id)).tag_id AS tag_id FROM nodes n) h JOIN objecttagrelations o ON o.tag_id = h.tag_id) N where N.parentnode_id = %i) R%i " % (
                        filts[i], i + 1))
                else:
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
            # print(types[i], filts[i])
        return endstr, frontstr, midstr

    @staticmethod
    def __execute_query(cursor, sqlstr):
        cursor.execute(sqlstr)
        return cursor.fetchall()

    def execute_query(self, sqlstr):
        with self.conn.cursor() as cursor:
            return self.__execute_query(cursor, sqlstr)

    @staticmethod
    def __drop_materialized_indexes(cursor):
        cursor.execute("DROP INDEX IF EXISTS tagsets_taggings_sid_oid_tid;")
        cursor.execute("DROP INDEX IF EXISTS nodes_taggings_pid_oid_nid;")
        cursor.execute("DROP INDEX IF EXISTS nodes_taggings_nid_oid;")

    def drop_materialized_indexes(self):
        with self.conn.cursor() as cursor:
            self.__drop_materialized_indexes(cursor)

    @staticmethod
    def __create_materialized_indexes(cursor):
        cursor.execute("create index tagsets_taggings_sid_oid_tid on tagsets_taggings (tagset_id, object_id, tag_id);")
        cursor.execute("create index nodes_taggings_pid_oid_nid on nodes_taggings (parentnode_id, object_id, node_id);")
        cursor.execute("create index nodes_taggings_nid_oid on nodes_taggings (node_id, object_id);")

    def create_materialized_indexes(self):
        with self.conn.cursor() as cursor:
            self.__create_materialized_indexes(cursor)

    @staticmethod
    def __insert_object(cursor, id, file_uri, file_type, thumbnail_uri):
        cursor.execute("INSERT INTO cubeobjects (id, file_uri, file_type, thumbnail_uri) VALUES (%s, %s, %s, %s);",
                       (id, file_uri, file_type, thumbnail_uri))

    def insert_object(self, id, file_uri, file_type, thumbnail_uri):
        with self.conn.cursor() as cursor:
            self.__insert_object(cursor, id, file_uri, file_type, thumbnail_uri)

    @staticmethod
    def __insert_tag(cursor, id, name, tagtype_id, tagset_id):
        cursor.execute("INSERT INTO tags (id, tagtype_id, tagset_id) VALUES (%s, %s, %s);", (id, tagtype_id, tagset_id))
        cursor.execute("INSERT INTO alphanumerical_tags (id,name,tagset_id) VALUES (%s, %s, %s);",
                       (id, name, tagset_id))

    def insert_tag(self, id, name, tagtype_id, tagset_id):
        with self.conn.cursor() as cursor:
            self.__insert_tag(cursor, id, name, tagtype_id, tagset_id)

    @staticmethod
    def __insert_tagset(cursor, id, name):
        cursor.execute("INSERT INTO tagsets (id, name, tagtype_id) VALUES (%s, %s, %s);", (id, name))

    def insert_tagset(self, id, name):
        with self.conn.cursor() as cursor:
            self.__insert_tagset(cursor, id, name)

    @staticmethod
    def __insert_node(cursor, id, tag_id, hierarchy_id):
        cursor.execute("INSERT INTO nodes (id, tag_id, hierarchy_id) VALUES (%s, %s, %s);", (id, tag_id, hierarchy_id))

    def insert_node(self, id, tag_id, hierarchy_id):
        with self.conn.cursor() as cursor:
            self.__insert_node(cursor, id, tag_id, hierarchy_id)

    @staticmethod
    def __tag_object(cursor, object_id, tag_id):
        cursor.execute("INSERT INTO objecttagrelations (object_id, tag_id) VALUES (%s, %s);", (object_id, tag_id))

    def tag_object(self, object_id, tag_id):
        with self.conn.cursor() as cursor:
            self.__tag_object(cursor, object_id, tag_id)

    @staticmethod
    def __update_object(cursor, id, file_uri, file_type, thumbnail_uri):
        cursor.execute("UPDATE cubeobjects SET file_uri = %s, file_type = %s, thumbnail_uri = %s WHERE id = %s;",
                       (file_uri, file_type, thumbnail_uri, id))

    def update_object(self, id, file_uri, file_type, thumbnail_uri):
        with self.conn.cursor() as cursor:
            self.__update_object(cursor, id, file_uri, file_type, thumbnail_uri)

    @staticmethod
    def __update_tag(cursor, id, name, tagtype_id, tagset_id):
        cursor.execute("UPDATE tags SET name = %s, tagtype_id = %s, tagset_id = %s WHERE id = %s;",
                       (name, tagtype_id, tagset_id, id))

    def update_tag(self, id, name, tagtype_id, tagset_id):
        with self.conn.cursor() as cursor:
            self.__update_tag(cursor, id, name, tagtype_id, tagset_id)

    def set_autocommit(self, autocommit):
        self.conn.autocommit = autocommit

    def rollback(self):
        self.conn.rollback()

    @staticmethod
    def __refresh_all_views(cursor):
        cursor.execute("REFRESH MATERIALIZED VIEW tagsets_taggings;")
        cursor.execute("REFRESH MATERIALIZED VIEW nodes_taggings;")
        cursor.execute("REFRESH MATERIALIZED VIEW flattened_hierarchies;")

    def refresh_all_views(self):
        with self.conn.cursor() as cursor:
            self.__refresh_all_views(cursor)

    @staticmethod
    def __refresh_object_views(cursor):
        cursor.execute("REFRESH MATERIALIZED VIEW tagsets_taggings;")
        cursor.execute("REFRESH MATERIALIZED VIEW nodes_taggings;")

    def refresh_object_views(self):
        with self.conn.cursor() as cursor:
            self.__refresh_object_views(cursor)

    @staticmethod
    def __get_node_tag_subtree(cursor, node_id):
        cursor.execute("SELECT * FROM get_subtree_from_parent_node(%s);", (node_id,))
        return cursor.fetchall()

    def get_node_tag_subtree(self, node_id):
        with self.conn.cursor() as cursor:
            return self.__get_node_tag_subtree(cursor, node_id)

    @staticmethod
    def __get_objects_with_tag(cursor, tag_id):
        cursor.execute("select R.object_id from objecttagrelations R where R.tag_id = %s;", (tag_id,))
        return cursor.fetchall()

    def get_objects_with_tag(self, tag_id):
        with self.conn.cursor() as cursor:
            return self.__get_objects_with_tag(cursor, tag_id)

    @staticmethod
    def __get_objects_in_tagset(cursor, tagset_id):
        cursor.execute("select T.object_id, T.tag_id as id from tagsets_taggings T where T.tagset_id = %s;",
                       (tagset_id,))
        return cursor.fetchall()

    def get_objects_in_tagset(self, tagset_id):
        with self.conn.cursor() as cursor:
            return self.__get_objects_in_tagset(cursor, tagset_id)

    @staticmethod
    def __get_objects_in_hierarchy(cursor, hierarchy_id):
        cursor.execute("select N.object_id, N.node_id as id from nodes_taggings N where N.parentnode_id = %s;",
                       (hierarchy_id,))
        return cursor.fetchall()

    def get_objects_in_subtree(self, hierarchy_id):
        with self.conn.cursor() as cursor:
            return self.__get_objects_in_hierarchy(cursor, hierarchy_id)
