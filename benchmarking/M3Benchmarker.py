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

import click
import psycopg
from matplotlib import pyplot as plt
from neo4j import GraphDatabase

import BenchmarkHarness
import Neo4JM3 as Neo4JM3
import PostgresqlM3 as PostgresqlM3
# Creating and Configuring Logger
from GraphCreator import create_latency_barchart, create_latency_scatter_plot, create_cbmi_latency_barchart
from dotenv import dotenv_values

env = dotenv_values(".env") 

LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(
    stream=sys.stdout,
    format=LOG_FORMAT,
    level=logging.INFO)

logger = logging.getLogger(__name__)

driver = GraphDatabase.driver(env["NEO4J_URL"], auth=(env["NEO4J_USER"], env["NEO4J_PASSWORD"]))
neo = Neo4JM3.Neo4jPC(driver)

PSQL_CONN = psycopg.connect(user=env["PSQL_USER"], password=env["PSQL_PASSWORD"], host=env["PSQL_HOST"], port=env["PSQL_PORT"], dbname=env["PSQL_DB"])
PSQL_CONN.autocommit = False  # allow for rollback on insert & update queries
psql = PostgresqlM3.PostgresqlPC(PSQL_CONN)

# LSC dataset max ids
MAX_TAG_ID = int(env["MAX_TAG_ID"])
MAX_TAGSET_ID = int(env["MAX_TAGSET_ID"])
MAX_HIERARCHY_ID = int(env["MAX_HIERARCHY_ID"])
MAX_NODE_ID = int(env["MAX_NODE_ID"])
MAX_OBJECT_ID = int(env["MAX_OBJECT_ID"])


@click.group()
def benchmark():
    pass

@benchmark.command("read")
@click.option("--r", default=10, help="Number of query repetitions")
def state_cell_benchmark(r):
    """Standard benchmarking of all read query comparisons.
    Runs all postgreSQL & Neo4J photocube read queries comparisons. Random ids used are the same for both databases to ensure fair comparison."""
    logger.info("Running standard latency benchmark with " + str(r) + " repetitions")
    results = {'query': [], 'latency': [], 'category': []}

    BenchmarkHarness.comp_bench_rand_id("Tag by id", psql.get_name(), neo.get_name(), psql.get_tag_by_id,
                                        neo.get_tag_by_id, r, MAX_TAG_ID, results)
    BenchmarkHarness.comp_bench_rand_id("Tags in tagset", psql.get_name(), neo.get_name(), psql.get_tags_in_tagset,
                                        neo.get_tags_in_tagset, r, MAX_TAGSET_ID, results)
    BenchmarkHarness.comp_bench_rand_id("Node tag subtree", psql.get_name(), neo.get_name(), psql.get_node_tag_subtree,
                                        neo.get_node_tag_subtree, r, MAX_NODE_ID, results)
    BenchmarkHarness.comp_random_state_benchmark(psql, neo, r, results)
    BenchmarkHarness.comp_random_cell_benchmark(psql, neo, r, results)


    title = "Latency of random Photocube read-only queries of Neo4j and Postgresql" + "\n" + "Query repetitions: %i " % r
    create_latency_barchart(results)
    plt.show()
    neo.close()
    psql.close()


@benchmark.command("state-cell")
@click.option("--r", default=10, help="Number of query repetitions")
def state_cell_benchmark(r):
    """Standard benchmarking of state & cell comparisons.
    Runs state & cell postgreSQL & Neo4J read queries comparisons. Random ids used are the same for both databases to ensure fair comparison."""
    logger.info("Running state & cell latency benchmark with " + str(r) + " repetitions")
    results = {'query': [], 'latency': [], 'category': []}

    BenchmarkHarness.comp_random_state_benchmark(psql, neo, r, results)
    BenchmarkHarness.comp_random_cell_benchmark(psql, neo, r, results)

    title = "Latency of state & cell queries of Neo4j and Postgresql" + "\n" + "Query repetitions: %i " % r
    create_cbmi_latency_barchart(results)
    plt.show()
    neo.close()
    psql.close()

@benchmark.command("complete")
@click.option("--r", default=10, help="Number of query repetitions")
@click.option("--w", is_flag=True, help="Benchmark insert & update queries? (default: False)")
def state_cell_benchmark(r, w):
    """Standard benchmarking for all comparisons.
    Runs all postgreSQL & Neo4J photocube queries comparisons. Random ids used are the same for both databases to ensure fair comparison."""
    logger.info("Running standard latency benchmark with " + str(r) + " repetitions")
    results = {'query': [], 'latency': [], 'category': []}

    BenchmarkHarness.comp_bench_rand_id("Tag by id", psql.get_name(), neo.get_name(), psql.get_tag_by_id,
                                        neo.get_tag_by_id, r, MAX_TAG_ID, results)
    BenchmarkHarness.comp_bench_rand_id("Tags in tagset", psql.get_name(), neo.get_name(), psql.get_tags_in_tagset,
                                        neo.get_tags_in_tagset, r, MAX_TAGSET_ID, results)
    BenchmarkHarness.comp_bench_rand_id("Node tag subtree", psql.get_name(), neo.get_name(), psql.get_node_tag_subtree,
                                        neo.get_node_tag_subtree, r, MAX_NODE_ID, results)
    BenchmarkHarness.comp_random_state_benchmark(psql, neo, r, results)
    BenchmarkHarness.comp_random_cell_benchmark(psql, neo, r, results)

    if w:
        BenchmarkHarness.insert_object_benchmark(psql, psql.get_name(), r, results)
        BenchmarkHarness.insert_object_benchmark(neo, neo.get_name(), r, results)

        BenchmarkHarness.insert_tag_benchmark(psql, psql.get_name(), r, results)
        BenchmarkHarness.insert_tag_benchmark(neo, neo.get_name(), r, results)

    title = "Latency of random Photocube queries of Neo4j and Postgresql" + "\n" + "Query repetitions: %i " % r
    create_latency_barchart(results)
    plt.show()
    neo.close()
    psql.close()


@benchmark.command(name="state")
@click.option("--r", default=10, help="Number of query repetitions")
@click.option("--neo4j", is_flag=True, help="Use only neo4j as database. Default is PostgreSQL only")
@click.option("--comp", is_flag=True, help="Compare both databases. Default is only one database")
def state_scenarios_benchmark(r, neo4j, comp):
    """Benchmarking for different state scenarios."""
    logger.info("Running state scenarios benchmark with " + str(r) + " repetitions")
    results = {'query': [], 'latency': [], 'category': []}

    if comp:
        logger.info("Comparing Neo4j & PostgreSQL as database for state scenarios")

        BenchmarkHarness.comp_2d_state_benchmark(psql, neo, "2D", r, results, incl_baseline=True)
        BenchmarkHarness.comp_3d_state_benchmark(psql, neo, "3D", r, results, incl_baseline=True)
        BenchmarkHarness.comp_3d_2f_state_benchmark(psql, neo, "3D + 2", r, results, incl_baseline=True)
    elif neo4j:
        logger.info("Using Neo4j as database for state scenarios")

        BenchmarkHarness.two_dimensions_state(neo, "2D", r, results, neo4j)
        BenchmarkHarness.three_dimensions_state(neo, "3D", r, results, neo4j)
        BenchmarkHarness.three_two_filters_dimensions_state(neo, "3D + 2", r, results, neo4j)
    else:
        BenchmarkHarness.two_dimensions_state(psql, "2D", r, results)
        BenchmarkHarness.three_dimensions_state(psql, "3D", r, results)
        BenchmarkHarness.three_two_filters_dimensions_state(psql, "3D + 2", r, results)

    title = 'Photocube state latency results \n Query repetitions:' + str(r) + ''
    create_latency_barchart(results)
    plt.show()
    neo.close()
    psql.close()


@benchmark.command(name="cbmi")
@click.option("--r", default=10, help="Number of query repetitions")
def state_scenarios_benchmark(r):
    """PostgreSQL only benchmarking for different state scenarios."""
    logger.info("Running CBMI state scenarios benchmark with " + str(r) + " repetitions")
    results = {'query': [], 'latency': [], 'category': []}

    BenchmarkHarness.two_dimensions_state(psql, "2D", r, results)
    BenchmarkHarness.three_dimensions_state(psql, "3D", r, results)
    BenchmarkHarness.three_two_filters_dimensions_state(psql, "3D + 2", r, results)

    create_cbmi_latency_barchart(results)

    plt.show()
    neo.close()
    psql.close()


@benchmark.command("write")
@click.option("--r", default=10, help="Number of query repetitions")
def write_latency_benchmark(r):
    """Benchmarking for write photocube scenarios."""
    logger.info("Running write latency benchmark with " + str(r) + " repetitions")
    results = {'query': [], 'latency': [], 'category': []}


    BenchmarkHarness.insert_object_benchmark(psql, psql.get_name() + " Baseline", r, results,refresh=False)
    BenchmarkHarness.insert_object_benchmark(psql, psql.get_name(), r, results)
    BenchmarkHarness.insert_object_benchmark(neo, neo.get_name(), r, results)

    BenchmarkHarness.insert_tag_benchmark(psql, psql.get_name() + " Baseline", r, results,refresh=False)
    BenchmarkHarness.insert_tag_benchmark(psql, psql.get_name(), r, results)
    BenchmarkHarness.insert_tag_benchmark(neo, neo.get_name(), r, results)

    title = "Latency of Photocube inserts of Neo4j and Postgresql" + "\n" + "Query repetitions: %i " % r
    create_latency_barchart(results)
    plt.show()
    neo.close()
    psql.close()


@benchmark.command("cells")
@click.option("--r", default=10, help="Number of query repetitions")
def cells_latency_benchmark(r):
    """Scatterplot of latency for different amount of cells returned in state queries."""
    logger.info("Running cells effect benchmark with " + str(r) + " repetitions")
    results = {'cells': [], 'latency': [], 'category': []}

    BenchmarkHarness.cell_number_state_benchmark(psql, neo, r, results)

    title = "Latency of random Photocube Neo4j and Postgresql state queries in relation to amount of cells returned" + "\n" + "Query samples per database: %i " % r
    create_latency_scatter_plot( results, "cells", "# Cells")

    plt.show()
    neo.close()
    psql.close()


@benchmark.command("cnt_sum")
@click.option("--r", default=10, help="Number of query repetitions")
def count_sum_latency_benchmark(r):
    """Scatterplot of latency for different amount of cells returned in state queries."""
    logger.info("Running object count effect benchmark with " + str(r) + " repetitions")
    results = {'total_cnt': [], 'latency': [], 'category': []}

    BenchmarkHarness.total_object_count_state_benchmark(psql, neo, r, results)

    title = "Latency of random Photocube Neo4j and Postgresql state queries in relation to object count sum" + "\n" + "Query samples per database: %i " % r
    create_latency_scatter_plot( results, "total_cnt", "Object count sum")

    plt.show()
    neo.close()
    psql.close()


@benchmark.command("max_dim")
@click.option("--r", default=10, help="Number of query repetitions")
def max_dim_latency_benchmark(r):
    """Scatterplot of latency for different max dimension objects returned in state queries."""
    logger.info("Running max dimension object count effect benchmark with " + str(r) + " repetitions")
    results = {'max_dim': [], 'latency': [], 'category': []}

    BenchmarkHarness.max_objects_dim_number_state_benchmark(psql, neo, r, results)

    title = "Latency of random Photocube Neo4j and Postgresql state queries in relation to max dimension object count " + "\n" + "Query samples per database: %i " % r
    create_latency_scatter_plot(results, "max_dim", "Max dimension object")

    plt.show()
    neo.close()
    psql.close()


if __name__ == '__main__':
    benchmark()
