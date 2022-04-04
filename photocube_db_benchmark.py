#!/usr/bin/env python3
"""
This is a benchmarking suite used to compare PostgreSQL and Neo4j photocube queries.

Dependencies:
pip install seaborn
pip install neo4j
pip install psycopg2
"""
import datetime
import statistics
import sys
import timeit
import random

import numpy as np
import pandas as pd
import seaborn as sns
__author__ = "Nikolaj Mertz"
# connect to neo4j & postgresql database
from matplotlib import pyplot as plt
from neo4j import GraphDatabase
from psycopg2 import connect


class Neo4jPhotocube:
    numdims = 0
    numtots = 0
    dims = []
    types = []
    filts = []

    def __init__(self, driver, session):
        self.driver = driver
        self.session = session

    @classmethod
    def apply_filters(cls,midstr):
        for i in range(cls.numdims, cls.numtots):
            if cls.types[i] == "S":
                midstr += "MATCH (fil%i_ts: Tagset {id: %i}) " % (i + 1, cls.filts[i])
                midstr += "MATCH (fil%i_ts)<-[:IN_TAGSET]-(R%i: Tag)<-[:TAGGED]-(o: Object)\n" % (i + 1, i + 1)
            elif cls.types[i] == "H":
                midstr += "MATCH (fil%i_n: Node {id: %i})\n" % (i + 1, cls.filts[i])
                midstr += "MATCH (fil%i_n)<-[:HAS_PARENT*]-(R%i : Node)-[:REPRESENTS]->(:Tag)<-[:TAGGED]-(o: Object)\n" % (
                i + 1, i + 1)
            elif Neo4jPhotocube.types[i] == "T":
                midstr += "MATCH (fil%i_t: Tag {id: %i})\n" % (i + 1, cls.filts[i])
                midstr += "MATCH (fil%i_t)<-[:TAGGED]-(o: Object)\n" % (i + 1)
            elif cls.types[i] == "M":
                midstr += "MATCH (m_fil%i_t: Tag) WHERE m_fil%i_t.id IN [" % (i + 1, i + 1)
                for j in range(len(cls.filts[i])):
                    if j == 0:
                        midstr += "%i" % (cls.filts[i][j])
                    else:
                        midstr += ",%i" % (cls.filts[i][j])
                midstr += "]\n"
                midstr += "MATCH (m_fil%i_t)<-[:TAGGED]-(o: Object)\n" % (i + 1)
            print(cls.types[i], cls.filts[i])
        return midstr

    @classmethod
    def apply_dimensions(cls,endstr, midstr):
        for i in range(cls.numdims):
            endstr += ("R%i.id as %s, " % (i + 1, cls.attrs[i]))

            if cls.types[i] == "S":
                midstr += "MATCH (dim%i_ts: Tagset {id: %i})\n" % (i + 1, cls.filts[i])
                midstr += "MATCH (dim%i_ts)<-[:IN_TAGSET]-(R%i: Tag)<-[:TAGGED]-(o: Object)\n" % (i + 1, i + 1)
            elif cls.types[i] == "H":
                midstr += "MATCH (dim%i_n: Node {id: %i})\n" % (i + 1, cls.filts[i])
                midstr += "MATCH (dim%i_n)<-[:HAS_PARENT*]-(R%i : Node)-[:REPRESENTS]->(:Tag)<-[:TAGGED]-(o: Object)\n" % (
                i + 1, i + 1)
            print(cls.types[i], cls.filts[i])
        return endstr, midstr

    @classmethod
    def gen_state_query(cls):
        attrs = ["idx", "idy", "idz"]

        frontstr = "" # add profile / explain here
        midstr = ""
        endstr = "RETURN "
        # handle empty dimensions
        for i in range(cls.numdims, 3):
            endstr += ("1 as %s, " % attrs[i])
        # apply dimensions
        endstr, midstr = cls.apply_dimensions(endstr, midstr)

        # apply rest of filters
        midstr = cls.apply_filters(midstr)

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
    

def gen_random_id(max_id):
    return random.randint(1, max_id)

def execute_benchmark(name, query_method, reps, max_id, result):
    print(name)
    for i in range(reps):
        start = datetime.datetime.now()
        query_method(session,gen_random_id(max_id))
        end = datetime.datetime.now()
        duration = end - start
        if name not in result:
            result[name] = []
        result[name].append(duration.total_seconds() * 1e3)
    return result


def show_values(axs, orient="v", space=.01):
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

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123"))
session = driver.session()
neo = Neo4jPhotocube(driver, session)


max_tag_id = 193189
max_tagset_id = 21
max_hierarchy_id = 3
max_node_id = 8842
max_object_id = 183386

raw_results = {}
reps = 10

execute_benchmark("get_tag_by_id", neo.get_tag_by_id, reps, max_tag_id, raw_results)
execute_benchmark("get_node_tag_subtree", neo.get_node_tag_subtree, reps, max_node_id, raw_results)
execute_benchmark("get_tags_in_tagset",neo.get_tags_in_tagset, reps, max_tagset_id, raw_results)

results = {'data': [],'sd': [], 'category': [], 'query': []}
for key in raw_results:
    results['data'].extend(
        [sum(raw_results[key]) / len(raw_results[key]), np.percentile(raw_results[key], 99),
         max(raw_results[key]), min(raw_results[key]), np.median(raw_results[key]), np.std(raw_results[key])])
    results['category'].extend(['avg', 'avg 99th', 'max', 'min', 'median', 'stdev'])
    results['query'].extend([key,key,key,key,key,key])

#print(results)

# seaborn bar plot of results
sns.set(style="darkgrid")
ax = sns.barplot(x="query", y="data",hue="category", data=results, log=True)
ax.set(xlabel='Query', ylabel='Time (ms) - log scale')
show_values(ax)

plt.show()




