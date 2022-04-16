import neo4j


class Neo4jPhotocube:
    """
    This class is used to connect to the Neo4j database and execute queries.
    """

    def __init__(self, driver):
        self.driver = driver

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    @staticmethod
    def __apply_filters(midstr, numdims, numtots, types, filts):
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
            #print(types[i], filts[i])
        return midstr

    @staticmethod
    def __apply_dimensions(endstr, midstr,attrs, numdims, types, filts):
        for i in range(numdims):
            endstr += ("R%i.id as %s, " % (i + 1, attrs[i]))

            if types[i] == "S":
                midstr += "MATCH (dim%i_ts: Tagset {id: %i})\n" % (i + 1, filts[i])
                midstr += "MATCH (dim%i_ts)<-[:IN_TAGSET]-(R%i: Tag)<-[:TAGGED]-(o: Object)\n" % (i + 1, i + 1)
            elif types[i] == "H":
                midstr += "MATCH (dim%i_n: Node {id: %i})\n" % (i + 1, filts[i])
                midstr += "MATCH (dim%i_n)<-[:HAS_PARENT*]-(R%i : Node)-[:REPRESENTS]->(:Tag)<-[:TAGGED]-(o: Object)\n" % (
                i + 1, i + 1)
            #print(types[i], filts[i])
        return endstr, midstr

    @staticmethod
    def gen_state_query(numdims, numtots, types, filts):
        attrs = ["idx", "idy", "idz"]

        frontstr = "" # add profile / explain here
        midstr = ""
        endstr = "RETURN "
        # handle empty dimensions
        for i in range(numdims, 3):
            endstr += ("1 as %s, " % attrs[i])
        # apply dimensions
        endstr, midstr = Neo4jPhotocube.__apply_dimensions(endstr, midstr,attrs, numdims, types, filts)

        # apply rest of filters
        midstr = Neo4jPhotocube.__apply_filters(midstr, numdims, numtots, types, filts)

        endstr += "max(o).file_uri as file_uri, count(o) as cnt;"

        neo4j_query = ("\n%s %s %s\n" % (frontstr, midstr, endstr))
        return neo4j_query

    def execute_query(self,query):
        with self.driver.session() as session:
            session.read_transaction(self.__execute_query, query)

    @staticmethod
    def __execute_query(tx, query):
        result = tx.run(query)
        return list(result)

    def get_tag_by_id(self,tag_id):
        with self.driver.session() as session:
            session.read_transaction(self.__get_tag_by_id, tag_id)

    @staticmethod
    def __get_tag_by_id(tx, tag_id):
        result = tx.run("MATCH (t:Tag {id: $tag_id}) RETURN t.name as name, labels(t)", tag_id=tag_id)
        return list(result)

    def get_tags_in_tagset(self,tagset_id):
        with self.driver.session() as session:
            session.read_transaction(self.__get_tags_in_tagset, tagset_id)

    @staticmethod
    def __get_tags_in_tagset(tx,tagset_id):
        result = tx.run("MATCH (t:Tag)-[:IN_TAGSET]->(ts:Tagset {id: $tagset_id}) RETURN t.id,t.name, labels(t), ts.id", tagset_id=tagset_id)
        return list(result)

    def get_level_from_parent_node(self,node_id,hierarchy_id):
        with self.driver.session() as session:
            session.read_transaction(self.__get_level_from_parent_node, node_id, hierarchy_id)

    @staticmethod
    def __get_level_from_parent_node(tx,node_id,hierarchy_id):
        result = tx.run("MATCH (root:Node {id: $node_id})<-[:HAS_PARENT]-(n : Node)-[:IN_HIERARCHY]->(h:Hierarchy {id: $hierarchy_id}) "
                        "MATCH (n)-[:REPRESENTS]->(t:Tag) "
                        "RETURN n.id, t.id, h.id, root.id", node_id=node_id, hierarchy_id=hierarchy_id)
        return list(result)

    def get_node_tag_subtree(self,node_id):
        with self.driver.session() as session:
            session.read_transaction(self.__get_node_tag_subtree, node_id)

    @staticmethod
    def __get_node_tag_subtree(tx, node_id):
        result = tx.run(
            "MATCH (root:Node {id: $node_id})<-[:HAS_PARENT]-(n : Node)-[:IN_HIERARCHY]->(h:Hierarchy) "
            "MATCH (n)-[:REPRESENTS]->(t:Tag) "
            "RETURN n.id, t.id, h.id, root.id", node_id=node_id)
        return list(result)