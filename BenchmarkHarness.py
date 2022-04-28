import datetime

import logging
import random
from PhotoCubeBenchmarker import  MAX_TAGSET_ID, MAX_NODE_ID, MAX_OBJECT_ID, MAX_TAG_ID
from PhotoCubeDatabaseInterface import PhotoCubeDB
from PostgresqlPhotocube import PostgresqlPC

logger = logging.getLogger(__name__)


def get_random_id(max_id):
    return random.randint(1, max_id)

def get_random_dimensions(type_options, S_filter_max, H_filter_max, numdims):
    types = []
    filts = []
    for i in range(numdims):
        types.append(type_options[random.randint(0, 1)])
        if types[i] == "S":
            filts.append(get_random_id(S_filter_max))
        else:
            filts.append(get_random_id(H_filter_max))
    return types,filts

def append_results(name,duration,category, result):
    result["query"].append(name)
    result["latency"].append(duration)
    result["category"].append(category)
    return result

def exec_bench_rand_id(name, category, query_method, reps, max_id, result):
    logger.info("Running " + name + " benchmark in " + category + " with " + str(reps) + " reps")
    for _ in range(reps):
        start = datetime.datetime.now()
        query_method(get_random_id(max_id))
        end = datetime.datetime.now()
        duration = end - start
        append_results(name, duration.total_seconds() * 1e3, category, result)
    return result

def comp_bench_rand_id(name, category1, category2, query_method1, query_method2, reps, max_id, result):
    logger.info("Running " + name + " benchmark in " + category1 + " & " + category2 + " with " + str(reps) + " reps")
    for _ in range(reps):
        rand_id = get_random_id(max_id)
        start = datetime.datetime.now()
        query_method1(rand_id)
        end = datetime.datetime.now()
        duration = end - start
        append_results(name, duration.total_seconds() * 1e3, category1, result)
        start = datetime.datetime.now()
        query_method2(rand_id)
        end = datetime.datetime.now()
        duration = end - start
        append_results(name, duration.total_seconds() * 1e3, category2, result)
    return result
    



def comp_random_state_benchmark(db1 : PhotoCubeDB, db2 : PhotoCubeDB, reps, result):
    logger.info("Running random state " + db1.get_name() + " & " + db2.get_name() + " benchmark with " + str(reps) + " reps")
    # generate different queries
    type_options = ["S", "H"]
    S_filter_max = MAX_TAGSET_ID
    H_filter_max = MAX_NODE_ID
    for _ in range(reps):
        numdims = 3
        numtots = 3
        types, filts = get_random_dimensions(type_options, S_filter_max, H_filter_max, numdims)

        #print(str(types) + "\n" + str(filts))
        query_name = "Random state"
        state_benchmark(db1,result, numdims, numtots, types, filts, query_name)
        state_benchmark(db2,result, numdims, numtots, types, filts, query_name)
    return result

def state_benchmark(db : PhotoCubeDB, result, numdims, numtots, types, filts, query_name="State"):
    start = datetime.datetime.now()
    db.execute_query(db.gen_state_query(numdims, numtots, types, filts))
    end = datetime.datetime.now()
    duration = end - start
    time = duration.total_seconds() * 1e3
    if time > 2000:
        logger.warning(db.get_name() + " state query time: " + str(round(time, 2)) + " ms" +
                  " parameters: " + str(types) + " " + str(filts))
    append_results(query_name, time, db.get_name(), result)

def random_state_benchmark(db : PhotoCubeDB,category, reps, result):
    logger.info("Running " + category + " benchmark with " + str(reps) + " reps")
    # generate different queries
    type_options = ["S", "H"]
    S_filter_max = MAX_TAGSET_ID
    H_filter_max = MAX_NODE_ID
    for _ in range(reps):
        numdims = 3
        numtots = 3
        types, filts = get_random_dimensions(type_options, S_filter_max, H_filter_max, numdims)

        #print(str(types) + "\n" + str(filts))
        state_benchmark(db,result, numdims, numtots, types, filts, "Random state")
    return result


def benchmark_string_query(db : PhotoCubeDB, reps, query, name, category, result):
    for _ in range(reps):
        start = datetime.datetime.now()
        db.execute_query(query)
        end = datetime.datetime.now()

        duration = (end - start).total_seconds() * 1e3
        append_results(name, duration, category, result)
    return result

def insert_object_benchmark(db : PhotoCubeDB, category, reps, result):
    logger.info("Running insert object benchmark in " + category + " with " + str(reps) + " reps")
    for i in range(MAX_OBJECT_ID+1,MAX_OBJECT_ID+1+reps):
        start = datetime.datetime.now()
        db.insert_object(i,"BENCHMARK_OBJECT_" + str(i),42, "BENCHMARK_OBJECT_" + str(i))
        db.refresh_object_views()
        end = datetime.datetime.now()
        duration = (end - start).total_seconds() * 1e3
        append_results("Insert object", duration, category, result)
    db.rollback()
    return result

def insert_tag_benchmark(db : PhotoCubeDB,category, reps, result):
    logger.info("Running insert tag benchmark in " + category + " with " + str(reps) + " reps")
    for i in range(MAX_TAG_ID+1,MAX_TAG_ID+1+reps):
        start = datetime.datetime.now()
        db.insert_tag(i,"BENCHMARK_TAG_" + str(i),1, MAX_TAGSET_ID)
        db.refresh_all_views()
        end = datetime.datetime.now()
        duration = (end - start).total_seconds() * 1e3
        append_results("Insert tag", duration, category, result)
    db.rollback()
    db.delete_all_benchmark_data()
    return result

"""PostgreSQL only benchmarks """

def baseline_materialize_index_benchmark(psql : PostgresqlPC, category, reps, result, numdims, numtots, types, filts):
    baseline_query = psql.gen_state_query(numdims, numtots, types, filts, baseline=True)
    materialized_view_query = psql.gen_state_query(numdims, numtots, types, filts)

    # drop materialized view indexes
    psql.drop_materialized_indexes()

    benchmark_string_query(psql, reps, baseline_query, "Baseline", category, result)
    benchmark_string_query(psql, reps, materialized_view_query, "Materialized Views", category, result)
    # create materialized view indexes
    psql.create_materialized_indexes()
    benchmark_string_query(psql, reps, materialized_view_query, "Indexed Views", category, result)
    return result

def lifelog_task_state_benchmark(psql,category, reps, result):
    """ 2D browsing state from Figure 1,
    with children of the Dog node on
    the one axis, and timezone on the
    other axis """
    numdims = 2
    numtots = 2
    types = ["H", "S"]
    filts = [40, 14]
    """
    Dim 1: 2 cells - 183386
    Dim 2: 29 cells - 183288
    Total Cells: 58
    C Time 640.622
    Row returned: 22
    Total Object count: 203202
    Smallest object count: x: 8434, y: 6092, z: 1 - 1
    """

    return baseline_materialize_index_benchmark(psql,category, reps, result, numdims, numtots, types, filts)

# simple
def simple_state_benchmark(psql,category, reps, result):
    # 2D browsing state with the top level of the
    # entity hierarchy on one axis and location on
    # the other axis
    numdims = 2
    numtots = 2
    types = ["H", "S"]
    filts = [40, 15]
    """
    Dim 1: 2 cells - 183386
    Dim 2: 161 cells - 127192
    Total Cells: 322

    C Time 608.773
    Row returned: 307
    Total Object count: 142027
    Smallest object count: x: 8434, y: 15612, z: 1 - 1
    """

    return baseline_materialize_index_benchmark(psql,category, reps, result, numdims, numtots, types, filts)


def medium_state_benchmark(psql, category, reps, result):
    """ 3D browsing state with the top level of the
    entity hierarchy on the first axis, the
    children of the Dog node on the second axis,
    and timezone on the third axis. """
    numdims = 3
    numtots = 3
    types = ["H", "H", "S"]
    filts = [40, 691, 14]

    """
    Dim 1: 2 cells 183386
    Dim 2: 13 cells 46
    Dim 3: 29 cells 183288
    Total Cells: 754
    C Time 69.244
    Row returned: 27
    Total Object count: 84
    Smallest object count: x: 41, y: 737, z: 17 - 1
    """


    return baseline_materialize_index_benchmark(psql,category, reps, result, numdims, numtots, types, filts)


def complex_state_benchmark(psql,category, reps, result):
    """ 3D browsing state with the top level of the
    entity hierarchy on the first axis, the
    children of the Dog node on the second axis,
    and location on the third axis. """
    numdims = 3
    numtots = 3
    types = ["H", "H", "S"]
    filts = [40, 691, 15]

    """
    Dim 1: 2 cells - 183386
    Dim 2: 13 cells - 46
    Dim 3: 161 cells - 127192
    Total Cells: 4186
    C Time 69.913
    Row returned: 43
    Total Object count: 62
    Smallest object count: x: 41, y: 737, z: 36 - 1
    """

    return baseline_materialize_index_benchmark(psql,category, reps, result, numdims, numtots, types, filts)

def two_dimensions_state(psql,category, reps, result):
    
    numdims = 2
    numtots = 2
    types = ["H", "S"]
    filts = [691, 11]

    #dog , year

    return baseline_materialize_index_benchmark(psql,category, reps, result, numdims, numtots, types, filts)




def three_dimensions_state(psql,category, reps, result):
    
    numdims = 3
    numtots = 3
    types = ["H","S","H"]
    filts = [691, 15,30]

    #dog , location, day of week string

    return baseline_materialize_index_benchmark(psql,category, reps, result, numdims, numtots, types, filts)

def three_two_filters_dimensions_state(psql,category, reps, result):
    
    numdims = 3
    numtots = 5
    types = ["S","H","S","H","T"]
    filts = [15,30,11,691,13]

   # location, day of week string, year, dog, september

    return baseline_materialize_index_benchmark(psql,category, reps, result, numdims, numtots, types, filts)