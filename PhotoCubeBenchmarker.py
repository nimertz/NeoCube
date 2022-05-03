#!/usr/bin/env python3
"""
This is a benchmarking suite used to compare PostgreSQL and Neo4j photocube queries.

Dependencies:
pip install numpy - for plotting custom barcharts
pip install click - for command line interface
pip install seaborn - for plotting
pip install neo4j - To install Neo4j driver 
pip install psycopg - To install PostgreSQL driver 
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
import BenchmarkHarness

# Creating and Configuring Logger
from GraphCreator import create_latency_barchart, create_latency_connected_scatter_plot

LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(
                    stream = sys.stdout, 
                    format = LOG_FORMAT,
                    level=logging.INFO)

logger = logging.getLogger(__name__)

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123"))
neo = Neo4JPhotocube.Neo4jPC(driver)

PSQL_CONN = psycopg.connect(user="photocube", password="123", host="127.0.0.1", port="5432", dbname="photocube")
PSQL_CONN.autocommit = False # allow for rollback on insert & update queries
psql = PostgresqlPhotocube.PostgresqlPC(PSQL_CONN)

# LSC dataset max ids
MAX_TAG_ID = 193189
MAX_TAGSET_ID = 21
MAX_HIERARCHY_ID = 3
MAX_NODE_ID = 8842
MAX_OBJECT_ID = 183386

@click.group()
def benchmark():
    pass

@benchmark.command("complete")
@click.option("--r", default=10, help="Number of query repetitions")
@click.option("--w", is_flag=True, help="Benchmark insert & update queries? (default: False)")
def standard_latency_benchmark(r, w):
    """Standard benchmarking suite for all comparisons.
    Runs all postgreSQL & Neo4J photocube queries comparisons. Random ids used are the same for both databases to ensure fair comparison."""
    logger.info("Running standard latency benchmark with " + str(r) + " repetitions")
    results = {'query': [], 'latency': [], 'category': []}

    BenchmarkHarness.comp_bench_rand_id("Tag by id", psql.get_name(), neo.get_name(), psql.get_tag_by_id, neo.get_tag_by_id, r, MAX_TAG_ID, results)
    BenchmarkHarness.comp_bench_rand_id("Tags in tagset", psql.get_name(), neo.get_name(), psql.get_tags_in_tagset, neo.get_tags_in_tagset, r, MAX_TAGSET_ID, results)
    BenchmarkHarness.comp_bench_rand_id("Node tag subtree", psql.get_name(), neo.get_name(), psql.get_node_tag_subtree, neo.get_node_tag_subtree, r, MAX_NODE_ID, results)
    BenchmarkHarness.comp_random_state_benchmark(psql,neo, r, results)

    if w:
        BenchmarkHarness.insert_object_benchmark(psql,psql.get_name(),r,results)
        BenchmarkHarness.insert_object_benchmark(neo,neo.get_name(),r,results)

        BenchmarkHarness.insert_tag_benchmark(psql,psql.get_name(),r,results)
        BenchmarkHarness.insert_tag_benchmark(neo,neo.get_name(),r,results)

    title = "Latency of Photocube queries of Neo4j and Postgresql" + "\n" + "Query repetitions: %i " % r
    create_latency_barchart(title, results)
    plt.show()
    neo.close()
    psql.close()

@benchmark.command(name="state")
@click.option("--r", default=10, help="Number of query repetitions")
@click.option("--neo4j", is_flag=True, help="Use only neo4j as database. Default is PostgreSQL only")
@click.option("--comp", is_flag=True, help="Compare both databases. Default is only one database")
def state_scenarios_benchmark(r, neo4j,comp):
    """Benchmarking suite for different state scenarios."""
    logger.info("Running state scenarios benchmark with " + str(r) + " repetitions")
    results = {'query': [], 'latency': [], 'category': []}

    if comp:
        logger.info("Comparing Neo4j & PostgreSQL as database for state scenarios")
        #BenchmarkHarness.simple_state_comp_benchmark(psql,neo,"Simple",r,results,incl_baseline=True)
        #BenchmarkHarness.medium_state_comp_benchmark(psql,neo,"Medium",r,results,incl_baseline=True)
        #BenchmarkHarness.complex_state_comp_benchmark(psql,neo,"Complex",r,results,incl_baseline=True)

        BenchmarkHarness.comp_2d_state_benchmark(psql,neo,"2D",r,results,incl_baseline=True)
        BenchmarkHarness.comp_3d_state_benchmark(psql,neo,"3D",r,results,incl_baseline=True)
        BenchmarkHarness.comp_3d_2f_state_benchmark(psql,neo,"3D + 2F",r,results,incl_baseline=True)
    elif neo4j:
        logger.info("Using Neo4j as database for state scenarios")
        #BenchmarkHarness.lifelog_task_state_benchmark(neo,"Lifelog Task", r, results,neo4j)
        #BenchmarkHarness.simple_state_benchmark(neo,"Simple", r, results,neo4j)
        #BenchmarkHarness.medium_state_benchmark(neo,"Medium", r, results,neo4j)
        #BenchmarkHarness.complex_state_benchmark(neo,"Complex", r, results,neo4j)

        BenchmarkHarness.two_dimensions_state(neo,"2D", r, results,neo4j)
        BenchmarkHarness.three_dimensions_state(neo,"3D", r, results, neo4j)
        BenchmarkHarness.three_two_filters_dimensions_state(neo,"3D + 2 filters", r, results, neo4j)
    else:
        #BenchmarkHarness.lifelog_task_state_benchmark(psql,"Lifelog Task", r, results)
        #BenchmarkHarness.simple_state_benchmark(psql,"Simple", r, results)
        #BenchmarkHarness.medium_state_benchmark(psql,"Medium", r, results)
        #BenchmarkHarness.complex_state_benchmark(psql,"Complex", r, results)

        BenchmarkHarness.two_dimensions_state(psql,"2D", r, results)
        BenchmarkHarness.three_dimensions_state(psql,"3D", r, results)
        BenchmarkHarness.three_two_filters_dimensions_state(psql,"3D + 2 filters", r, results)

    title ='Photocube state latency results \n Query repetitions:' + str(r) + ''
    create_latency_barchart(title,results)
    plt.show()
    neo.close()
    psql.close()

@benchmark.command("write")
@click.option("--r", default=10, help="Number of query repetitions")
def write_latency_benchmark(r):
    """Benchmarking suite for write photocube scenarios."""
    logger.info("Running write latency benchmark with " + str(r) + " repetitions")
    results = {'query': [], 'latency': [], 'category': []}

    BenchmarkHarness.insert_object_benchmark(psql,psql.get_name(),r,results)
    BenchmarkHarness.insert_object_benchmark(neo,neo.get_name(),r,results)

    BenchmarkHarness.insert_tag_benchmark(psql,psql.get_name(),r,results)
    BenchmarkHarness.insert_tag_benchmark(neo,neo.get_name(),r,results)

    title = "Latency of Photocube inserts of Neo4j and Postgresql" + "\n" + "Query repetitions: %i " % r
    create_latency_barchart(title, results)
    plt.show()
    neo.close()
    psql.close()

@benchmark.command("cells")
@click.option("--r", default=10, help="Number of query repetitions")
def write_latency_benchmark(r):
    """Connected scatterplot of latency for different amount of cells returned in state queries."""
    logger.info("Running cells effect benchmark with " + str(r) + " repetitions")
    results = {'cells': [], 'latency': [], 'category': []}

    BenchmarkHarness.cell_number_state_benchmark(psql,neo,r,results)

    title = "Latency of Photocube Neo4j and Postgresql state queries in relation to amount of cells returned" + "\n" + "Query samples per database: %i " % r
    create_latency_connected_scatter_plot(title,results,"cells", "# Cells")

    plt.show()
    neo.close()
    psql.close()


if __name__ == '__main__':
    benchmark()
