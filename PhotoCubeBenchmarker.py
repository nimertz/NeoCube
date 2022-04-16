#!/usr/bin/env python3
"""
This is a benchmarking suite used to compare PostgreSQL and Neo4j photocube queries.

Dependencies:
pip install numpy - for plotting custom barcharts
pip install click - for command line interface
pip install seaborn - for plotting
pip install neo4j - To install Neo4j driver (4.4.0)
pip install psycopg - To install PostgreSQL driver (3.0.11)
"""
__author__ = "Nikolaj Mertz"

import logging
import sys

from matplotlib import pyplot as plt
import click
import psycopg
from neo4j import GraphDatabase

import Neo4JPhotocube
import PostgresqlPhotocube
import PhotoCubeBenchmarkHarness

# Creating and Configuring Logger
from GraphCreator import create_barchart

LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(
                    stream = sys.stdout, 
                    format = LOG_FORMAT,
                    level=logging.INFO)

logger = logging.getLogger(__name__)

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123"))
neo = Neo4JPhotocube.Neo4jPhotocube(driver)

PSQL_CONN = psycopg.connect(user="photocube", password="123", host="127.0.0.1", port="5432", dbname="photocube")
psql = PostgresqlPhotocube.PostgresqlPhotocube(PSQL_CONN)

MAX_TAG_ID = 193189
MAX_TAGSET_ID = 21
MAX_HIERARCHY_ID = 3
MAX_NODE_ID = 8842
MAX_OBJECT_ID = 183386

@click.group()
def benchmark():
    pass

@benchmark.command("complete")
@click.option("--r", default=10, help="Number of repetitions")
def standard_latency_benchmark(r):
    logger.info("Running standard latency benchmark with " + str(r) + " repetitions")
    results = {'query': [], 'latency': [], 'category': []}

    PhotoCubeBenchmarkHarness.exec_bench_rand_id("Tag by id", "PostgreSQL", psql.get_tag_by_id, r, MAX_TAG_ID, results)
    PhotoCubeBenchmarkHarness.exec_bench_rand_id("Tag by id", "Neo4j", neo.get_tag_by_id, r, MAX_TAG_ID, results)

    PhotoCubeBenchmarkHarness.exec_bench_rand_id("Tags in tagset", "PostgreSQL", psql.get_tags_in_tagset, r, MAX_TAGSET_ID, results)
    PhotoCubeBenchmarkHarness.exec_bench_rand_id("Tags in tagset", "Neo4j", neo.get_tags_in_tagset, r, MAX_TAGSET_ID, results)

    PhotoCubeBenchmarkHarness.exec_bench_rand_id("Node tag subtree", "Neo4j", neo.get_node_tag_subtree, r, MAX_NODE_ID, results)

    PhotoCubeBenchmarkHarness.comp_random_state_benchmark(r, results)

    title = "Latency of Photocube queries of Neo4j and Postgresql" + "\n" + "Query repetitions: %i " % r
    create_barchart(title, results)
    plt.show()
    neo.close()
    psql.close()

@benchmark.command(name="state")
@click.option("--r", default=10, help="Number of repetitions")
def state_scenarios_and_progression_benchmark(r):
    logger.info("Running state scenarios and progression benchmark with " + str(r) + " repetitions")
    results = {'query': [], 'latency': [], 'category': []}

    PhotoCubeBenchmarkHarness.simple_state_benchmark("Simple", r, results)
    PhotoCubeBenchmarkHarness.medium_state_benchmark("Medium", r, results)
    PhotoCubeBenchmarkHarness.complex_state_benchmark("Complex", r, results)

    title ='Photocube state latency results \n Query repetitions:' + str(r) + ''
    create_barchart(title,results)
    plt.show()
    neo.close()
    psql.close()

if __name__ == '__main__':
    benchmark()
