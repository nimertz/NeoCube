import datetime

import logging
import random
from PhotoCubeBenchmarker import psql, neo, MAX_TAGSET_ID, MAX_NODE_ID

logger = logging.getLogger(__name__)

def get_random_id(max_id):
    return random.randint(1, max_id)

def exec_bench_rand_id(name, category, query_method, reps, max_id, result):
    logger.info("Running " + name + " benchmark in " + category + " with " + str(reps) + " reps")
    for _ in range(reps):
        start = datetime.datetime.now()
        query_method(get_random_id(max_id))
        end = datetime.datetime.now()
        duration = end - start
        result["query"].append(name)
        result["latency"].append(duration.total_seconds() * 1e3)
        result["category"].append(category)
    return result

def comp_random_state_benchmark(reps,result):
    # generate different queries
    type_options = ["S", "H"]
    S_filter_max = MAX_TAGSET_ID
    H_filter_max = MAX_NODE_ID
    logger.info("Running state neo4j & postgresql benchmark with " + str(reps) + " reps")
    for i in range(reps):
        numdims = 3
        numtots = 3
        types = []
        filts = []
        for i in range(numdims):
            types.append(type_options[random.randint(0, 1)])
            if types[i] == "S":
                filts.append(get_random_id(S_filter_max))
            else:
                filts.append(get_random_id(H_filter_max))

        #print(str(types) + "\n" + str(filts))

        start = datetime.datetime.now()
        neo.execute_query(neo.gen_state_query(numdims, numtots, types, filts))
        end = datetime.datetime.now()
        duration = end - start
        neo4j_time = duration.total_seconds() * 1e3
        if neo4j_time > 2000:
            logger.warning("Neo4j time: " + str(round(neo4j_time, 2)) + " ms" +
                  " query: " + str(types) + " " + str(filts))
        result["query"].append("Random state")
        result["latency"].append(neo4j_time)
        result["category"].append("Neo4j")

        start = datetime.datetime.now()
        psql.execute_query(psql.gen_state_query(numdims, numtots, types, filts))
        end = datetime.datetime.now()

        duration = end - start
        psql_time = duration.total_seconds() * 1e3
        if psql_time > 2000:
            logger.warning("PostgreSQL time: " + str(round(psql_time, 2)) + " ms" +
                  " query: " + str(types) + " " + str(filts))
        result["query"].append("Random state")
        result["latency"].append(psql_time)
        result["category"].append("PostgreSQL")

    return result

def random_state_benchmark(db,category, reps, result):
    # generate different queries
    type_options = ["S", "H"]
    S_filter_max = MAX_TAGSET_ID
    H_filter_max = MAX_NODE_ID
    logger.info("Running " + category + " benchmark with " + str(reps) + " reps")
    for i in range(reps):
        numdims = 3
        numtots = 3
        types = []
        filts = []
        for i in range(numdims):
            types.append(type_options[random.randint(0, 1)])
            if types[i] == "S":
                filts.append(get_random_id(S_filter_max))
            else:
                filts.append(get_random_id(H_filter_max))

        #print(str(types) + "\n" + str(filts))

        start = datetime.datetime.now()
        db.execute_query(db.gen_state_query(numdims, numtots, types, filts))
        end = datetime.datetime.now()
        duration = end - start
        time = duration.total_seconds() * 1e3
        if time > 2000:
            print("time: " + str(round(time, 2)) + " ms" +
                  " query: " + str(types) + " " + str(filts))
        result["query"].append("Random state")
        result["latency"].append(time)
        result["category"].append(category)
    return result

def state_bench(db,reps, query, name, category, result):
    for i in range(reps):
        start = datetime.datetime.now()
        db.execute_query(query)
        end = datetime.datetime.now()

        duration = (end - start).total_seconds() * 1e3
        result["query"].append(name)
        result["latency"].append(duration)
        result["category"].append(category)
    return result


def baseline_materialize_index_benchmark(category, reps, result, numdims, numtots, types, filts):
    baselineQuery = psql.gen_state_query(numdims, numtots, types, filts, baseline=True)
    materializedViewQuery = psql.gen_state_query(numdims, numtots, types, filts)

    # drop materialized view indexes
    psql.drop_materialized_indexes()

    state_bench(psql,reps, baselineQuery, "Baseline", category, result)
    state_bench(psql,reps, materializedViewQuery, "Materialized Views", category, result)
    # create materialized view indexes
    psql.create_materialized_indexes()
    state_bench(psql,reps, materializedViewQuery, "Indexed Views", category, result)
    return result


# simple
def simple_state_benchmark(category, reps, result):
    # 2D browsing state with the top level of the
    # entity hierarchy on one axis and location on
    # the other axis
    numdims = 2
    numtots = 2
    types = ["H", "S"]
    filts = [40, 15]

    return baseline_materialize_index_benchmark(category, reps, result, numdims, numtots, types, filts)


def medium_state_benchmark(category, reps, result):
    """ 3D browsing state with the top level of the
    entity hierarchy on the first axis, the
    children of the Dog node on the second axis,
    and timezone on the third axis. """
    numdims = 3
    numtots = 3
    types = ["H", "H", "S"]
    filts = [40, 5, 14]

    return baseline_materialize_index_benchmark(category, reps, result, numdims, numtots, types, filts)


def complex_state_benchmark(category, reps, result):
    """ 3D browsing state with the top level of the
    entity hierarchy on the first axis, the
    children of the Dog node on the second axis,
    and location on the third axis. """
    numdims = 3
    numtots = 3
    types = ["H", "H", "S"]
    filts = [40, 5, 15]

    return baseline_materialize_index_benchmark(category, reps, result, numdims, numtots, types, filts)