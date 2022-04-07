#!/usr/bin/env python3
"""
This is a benchmarking suite used to compare PostgreSQL and Neo4j photocube queries.

Dependencies:
pip install numpy
pip install seaborn
pip install neo4j
pip install psycopg2
"""
import datetime
import random

import numpy as np
import pandas as pd
import psycopg2
import seaborn as sns
__author__ = "Nikolaj Mertz"
# connect to neo4j & postgresql database
from matplotlib import pyplot as plt
from neo4j import GraphDatabase



class Neo4jPhotocube:
    """
    This class is used to connect to the Neo4j database and execute queries.
    """

    def __init__(self, driver, session):
        self.driver = driver
        self.session = session

    @staticmethod
    def apply_filters(midstr, numdims, numtots, types, filts):
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
    def apply_dimensions(endstr, midstr,attrs, numdims, types, filts):
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
        endstr, midstr = Neo4jPhotocube.apply_dimensions(endstr, midstr,attrs, numdims, types, filts)

        # apply rest of filters
        midstr = Neo4jPhotocube.apply_filters(midstr, numdims, numtots, types, filts)

        endstr += "max(o).file_uri as file_uri, count(o) as cnt;"

        neo4j_query = ("\n%s %s %s\n" % (frontstr, midstr, endstr))
        return neo4j_query

    @staticmethod
    def query_state(tx, query):
        result = tx.run(query)
        return list(result)


    @staticmethod
    def get_tag_by_id(tx, tag_id):
        result = tx.run("MATCH (t:Tag {id: $tag_id}) RETURN t.name as name, labels(t)", tag_id=tag_id)
        return list(result)

    @staticmethod
    def get_tags_in_tagset(tx,tagset_id):
        result = tx.run("MATCH (t:Tag)-[:IN_TAGSET]->(ts:Tagset {id: $tagset_id}) RETURN t.id,t.name, labels(t), ts.id", tagset_id=tagset_id)
        return list(result)

    @staticmethod
    def get_level_from_parent_node(tx,node_id,hierarchy_id):
        result = tx.run("MATCH (root:Node {id: $node_id})<-[:HAS_PARENT]-(n : Node)-[:IN_HIERARCHY]->(h:Hierarchy {id: $hierarchy_id}) "
                        "MATCH (n)-[:REPRESENTS]->(t:Tag) "
                        "RETURN n.id, t.id, h.id, root.id", node_id=node_id, hierarchy_id=hierarchy_id)
        return list(result)

    @staticmethod
    def get_node_tag_subtree(tx, node_id):
        result = tx.run(
            "MATCH (root:Node {id: $node_id})<-[:HAS_PARENT]-(n : Node)-[:IN_HIERARCHY]->(h:Hierarchy) "
            "MATCH (n)-[:REPRESENTS]->(t:Tag) "
            "RETURN n.id, t.id, h.id, root.id", node_id=node_id)
        return list(result)

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


def gen_random_id(max_id):
    return random.randint(1, max_id)

def execute_benchmark(name, session, query_method, reps, max_id, result):
    print("Running " + name + " benchmark with " + str(reps) + " reps")
    for i in range(reps):
        start = datetime.datetime.now()
        query_method(session,gen_random_id(max_id))
        end = datetime.datetime.now()
        duration = end - start
        if name not in result:
            result[name] = []
        result[name].append(duration.total_seconds() * 1e3)
    return result

def neo4j_state_benchmark(name, reps, result):
    # generate different queries
    type_options = ["S", "H"]
    S_filter_max = max_tagset_id
    H_filter_max = max_node_id
    print("Running " + name + " benchmark with " + str(reps) + " reps")
    for i in range(reps):
        numdims = 3
        numtots = 3
        types = []
        filts = []
        for i in range(numdims):
            types.append(type_options[random.randint(0, 1)])
            if types[i] == "S":
                filts.append(gen_random_id(S_filter_max))
            else:
                filts.append(gen_random_id(H_filter_max))

        #print(str(types) + "\n" + str(filts))

        start = datetime.datetime.now()
        Neo4jPhotocube.query_state(session, Neo4jPhotocube.gen_state_query(numdims, numtots, types, filts))
        end = datetime.datetime.now()
        duration = end - start
        time = duration.total_seconds() * 1e3
        if time > 2000:
            print("time: " + str(round(time, 2)) + " ms" +
                  " query: " + str(types) + " " + str(filts))
        if name not in result:
            result[name] = []
        result[name].append(time)
    return result

def postgresql_state_benchmark(name, reps, result):
    # generate different queries
    type_options = ["S", "H"]
    S_filter_max = max_tagset_id
    H_filter_max = max_node_id
    print("Running " + name + " benchmark with " + str(reps) + " reps")
    for i in range(reps):
        numdims = 3
        numtots = 3
        types = []
        filts = []
        for i in range(numdims):
            types.append(type_options[random.randint(0, 1)])
            if types[i] == "S":
                filts.append(gen_random_id(S_filter_max))
            else:
                filts.append(gen_random_id(H_filter_max))

        #print(str(types) + "\n" + str(filts))

        start = datetime.datetime.now()
        PostgresqlPhotocube.execute_query(psql.cursor,PostgresqlPhotocube.gen_state_query(numdims, numtots, types, filts))
        end = datetime.datetime.now()
        duration = end - start
        time = duration.total_seconds() * 1e3
        if time > 2000:
            print("time: " + str(round(time, 2)) + " ms" +
                  " query: " + str(types) + " " + str(filts))
        if name not in result:
            result[name] = []
        result[name].append(time)
    return result

def show_barchart_values(axs, orient="v", space=.01):
    def _single(ax):
        if orient == "v":
            for p in ax.patches:
                _x = p.get_x() + p.get_width() / 2
                _y = p.get_y() + p.get_height() + (p.get_height()*0.01)
                value = '{:.1f}'.format(p.get_height())
                ax.text(_x, _y, value, ha="center")
        elif orient == "h":
            for p in ax.patches:
                _x = p.get_x() + p.get_width() + float(space)
                _y = p.get_y() + p.get_height() - (p.get_height()*0.5)
                value = '{:.1f}'.format(p.get_width())
                ax.text(_x, _y, value, ha="left")

    if isinstance(axs, np.ndarray):
        for idx, ax in np.ndenumerate(axs):
            _single(ax)
    else:
        _single(axs)

def format_data_barchart(raw_results):
    results = {'data': [], 'sd': [], 'category': [], 'query': []}
    for key in raw_results:
        results['data'].extend(
            [sum(raw_results[key]) / len(raw_results[key]), np.percentile(raw_results[key], 99),
             max(raw_results[key]), min(raw_results[key]), np.median(raw_results[key]), np.std(raw_results[key])])
        results['sd'].extend([np.std(raw_results[key])] * 6)
        results['category'].extend(['avg', 'avg 99th', 'max', 'min', 'median', 'stdev'])
        results['query'].extend([key] * 6)
    return results

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123"))
session = driver.session()
neo = Neo4jPhotocube(driver, session)

psql_conn = psycopg2.connect(user="photocube", password="123", host="127.0.0.1", port="5432", database="photocube")
psql = PostgresqlPhotocube(psql_conn)

max_tag_id = 193189
max_tagset_id = 21
max_hierarchy_id = 3
max_node_id = 8842
max_object_id = 183386

raw_results = {}
reps = 10

execute_benchmark("neo4j_get_tag_by_id",session, neo.get_tag_by_id, reps, max_tag_id, raw_results)
execute_benchmark("neo4j_get_node_tag_subtree",session, neo.get_node_tag_subtree, reps, max_node_id, raw_results)
execute_benchmark("neo4j_get_tags_in_tagset",session,neo.get_tags_in_tagset, reps, max_tagset_id, raw_results)
neo4j_state_benchmark("neo4j_get_state", reps, raw_results)

execute_benchmark("psql_get_tag_by_id", psql.cursor, psql.get_tag_by_id, reps, max_tag_id, raw_results)
execute_benchmark("psql_get_tags_in_tagset", psql.cursor, psql.get_tags_in_tagset, reps, max_tagset_id, raw_results)
postgresql_state_benchmark("psql_get_state", reps, raw_results)

results = format_data_barchart(raw_results)

#print(results)

# seaborn bar plot of results
sns.set(style="darkgrid")
sns.despine()
ax = sns.barplot(x="query", y="data",hue="category", data=results, log=True,ci="sd",palette="Set2")
ax.set(xlabel='Query', ylabel='Time (ms) - log scale', title='Photocube latency results')
show_barchart_values(ax)
#TODO set standard deviation error bars on the barchart

plt.show()



