import datetime
import logging
import random

from M3Benchmarker import MAX_TAGSET_ID, MAX_NODE_ID, MAX_OBJECT_ID, MAX_TAG_ID
from M3DatabaseInterface import M3DB
from PostgresqlM3 import PostgresqlPC

logger = logging.getLogger(__name__)


def __get_random_id(max_id):
    return random.randint(1, max_id)


def __get_random_dimensions(type_options, numdims):
    types = []
    filts = []
    for i in range(numdims):
        types.append(type_options[random.randint(0, len(type_options) - 1)])
        __get_filter_ids_from_types(types, filts, i)
    return types, filts

def __get_filter_ids_from_types(types, filts, i):
    if types[i] == "S":
        filts.append(__get_random_id(MAX_TAGSET_ID))
    elif types[i] == "H":
        filts.append(__get_random_id(MAX_NODE_ID))
    elif types[i] == "T":
        filts.append(__get_random_id(MAX_TAG_ID))


def __append_metric_results(metric_name, metric, duration, category, result):
    result[metric_name].append(metric)
    result["latency"].append(duration)
    result["category"].append(category)
    return result


def __append_results(query_name, duration, category, result):
    result["query"].append(query_name)
    result["latency"].append(duration)
    result["category"].append(category)
    return result


def exec_bench_rand_id(name, category, query_method, reps, max_id, result):
    logger.info("Running " + name + " benchmark in " + category + " with " + str(reps) + " reps")
    for _ in range(reps):
        start = datetime.datetime.now()
        query_method(__get_random_id(max_id))
        end = datetime.datetime.now()
        duration = end - start
        __append_results(name, duration.total_seconds() * 1e3, category, result)
    return result


def comp_bench_rand_id(name, category1, category2, query_method1, query_method2, reps, max_id, result):
    logger.info("Running " + name + " benchmark in " + category1 + " & " + category2 + " with " + str(reps) + " reps")
    for _ in range(reps):
        rand_id = __get_random_id(max_id)
        start = datetime.datetime.now()
        query_method1(rand_id)
        end = datetime.datetime.now()
        duration = end - start
        __append_results(name, duration.total_seconds() * 1e3, category1, result)
        start = datetime.datetime.now()
        query_method2(rand_id)
        end = datetime.datetime.now()
        duration = end - start
        __append_results(name, duration.total_seconds() * 1e3, category2, result)
    return result

def comp_random_state_types_benchmark(db1: M3DB, db2: M3DB, reps, result):
    logger.info(
        "Running random state types " + db1.get_name() + " & " + db2.get_name() + " benchmark with " + str(reps) + " reps")
    # generate different queries
    numdims = 3
    numtots = 3
    for _ in range(reps):
        types = ["H", "H","H"]
        filts = []
        for i in range(numdims):
            __get_filter_ids_from_types(types, filts, i)
        
        query_name = "3 Node"
        state_benchmark(db1, result, numdims, numtots, types, filts, db1.get_name(), query_name)
        state_benchmark(db2, result, numdims, numtots, types, filts, db2.get_name(), query_name)

        types = ["H", "H","S"]
        filts = []
        for i in range(numdims):
            __get_filter_ids_from_types(types, filts, i)
        
        query_name = "2 Node + 1 Tagset"
        state_benchmark(db1, result, numdims, numtots, types, filts, db1.get_name(), query_name)
        state_benchmark(db2, result, numdims, numtots, types, filts, db2.get_name(), query_name)

        types = ["H", "S","S"]
        filts = []
        for i in range(numdims):
            __get_filter_ids_from_types(types, filts, i)
        
        query_name = "1 Node + 2 Tagset"
        state_benchmark(db1, result, numdims, numtots, types, filts, db1.get_name(), query_name)
        state_benchmark(db2, result, numdims, numtots, types, filts, db2.get_name(), query_name)
    return result

def comp_random_state_benchmark(db1: M3DB, db2: M3DB, reps, result):
    logger.info(
        "Running random state " + db1.get_name() + " & " + db2.get_name() + " benchmark with " + str(reps) + " reps")
    # generate different queries
    type_options = ["S", "H"]
    numdims = 3
    numtots = 3
    for _ in range(reps):
        types, filts = __get_random_dimensions(type_options, numdims)
        while (types == ['S', 'S', 'S']):
            types, filts = __get_random_dimensions(type_options, numdims)

        query_name = "3D state"
        state_benchmark(db1, result, numdims, numtots, types, filts, db1.get_name(), query_name)
        state_benchmark(db2, result, numdims, numtots, types, filts, db2.get_name(), query_name)
    return result

def comp_random_cell_benchmark(db1: M3DB, db2: M3DB, reps, result):
    logger.info(
        "Running random cell " + db1.get_name() + " & " + db2.get_name() + " benchmark with " + str(reps) + " reps")
    # generate different queries
    type_options = ["H","T"]
    numdims = 3
    numtots = 3
    for _ in range(reps):
        types, filts = __get_random_dimensions(type_options, numdims)
        while (types == ['S', 'S', 'S']):
            types, filts = __get_random_dimensions(type_options, numdims)

        query_name = "3D cell"
        logger.info("cell query generation using " + str(types) + " and " + str(filts))
        db1_cell_query = db1.gen_cell_query(numdims, numtots, types, filts)
        db2_cell_query = db2.gen_cell_query(numdims, numtots, types, filts)

        benchmark_string_query(db1,1, db1_cell_query, query_name,db1.get_name(),result)
        benchmark_string_query(db2, 1, db2_cell_query, query_name, db2.get_name(), result)

    return result



def cell_number_state_benchmark(db1: M3DB, db2: M3DB, reps, result):
    logger.info(
        "Running cell number effect of state query for " + db1.get_name() + " & " + db2.get_name() + " benchmark with " + str(
            reps) + " reps")

    type_options = ["S", "H"]
    numdims = 3
    numtots = 3
    for _ in range(reps):
        types, filts = __get_random_dimensions(type_options, numdims)

        rows1, time1, rows2, time2 = __time_dbs_state_query(db1, db2, numdims, numtots, types, filts)

        __append_metric_results("cells", len(rows1), time1, db1.get_name(), result)
        __append_metric_results("cells", len(rows2), time2, db2.get_name(), result)

    return result


def max_objects_dim_number_state_benchmark(db1: M3DB, db2: M3DB, reps, result):
    logger.info(
        "Running max objects dimension effect of state query for " + db1.get_name() + " & " + db2.get_name() + " benchmark with " + str(
            reps) + " reps")

    type_options = ["S", "H"]
    numdims = 3
    numtots = 3
    for i in range(reps):
        types, filts = __get_random_dimensions(type_options, numdims)

        max_object_count = 1
        for dimType, id in zip(types, filts):
            if dimType == "S":
                db1_cnt = len(db1.get_objects_in_tagset(id))
                max_object_count += db1_cnt
            elif dimType == "H":
                db1_cnt = len(db1.get_objects_in_subtree(id))
                max_object_count += db1_cnt

        rows1, time1, rows2, time2 = __time_dbs_state_query(db1, db2, numdims, numtots, types, filts)

        if i % 25 == 0:
            logger.info("max objects dimension effect at rep: " + str(i))

        logger.info("Max objects for dim: " + str(max_object_count))
        __append_metric_results("max_dim", max_object_count, time1, db1.get_name(), result)
        __append_metric_results("max_dim", max_object_count, time2, db2.get_name(), result)

    return result


def total_object_count_state_benchmark(db1: M3DB, db2: M3DB, reps, result):
    logger.info(
        "Running object count sum effect of state query for " + db1.get_name() + " & " + db2.get_name() + " benchmark with " + str(
            reps) + " reps")

    type_options = ["S", "H"]
    numdims = 3
    numtots = 3
    for _ in range(reps):
        types, filts = __get_random_dimensions(type_options, numdims)

        rows1, time1, rows2, time2 = __time_dbs_state_query(db1, db2, numdims, numtots, types, filts)

        __append_metric_results("total_cnt", __sum_state_cnt(rows1), time1, db1.get_name(), result)
        __append_metric_results("total_cnt", __sum_state_cnt(rows2), time2, db2.get_name(), result)

    return result


def __time_dbs_state_query(db1, db2, numdims, numtots, types, filts):
    start = datetime.datetime.now()
    rows1 = db1.execute_query(db1.gen_state_query(numdims, numtots, types, filts))
    end = datetime.datetime.now()
    duration = end - start
    time1 = duration.total_seconds() * 1e3
    if time1 > 2000:
        logger.warning(db1.get_name() + " state query time: " + str(round(time1, 2)) + " ms" +
                       " parameters: " + str(types) + " " + str(filts))

    start = datetime.datetime.now()
    rows2 = db2.execute_query(db2.gen_state_query(numdims, numtots, types, filts))
    end = datetime.datetime.now()
    duration = end - start
    time2 = duration.total_seconds() * 1e3
    if time2 > 2000:
        logger.warning(db2.get_name() + " state query time: " + str(round(time2, 2)) + " ms" +
                       " parameters: " + str(types) + " " + str(filts))

    return rows1, time1, rows2, time2


def __sum_state_cnt(rows):
    cnt_sum = 0
    for row in rows:
        cnt_sum += row[4]
    return cnt_sum


def state_benchmark(db: M3DB, result, numdims, numtots, types, filts, category, query_name="State"):
    logger.info(
        db.get_name() + ": State query benchmark using:\n numdims=%i, numtots=%i, types=%s, filts=%s" % (numdims, numtots, types, filts))
    start = datetime.datetime.now()
    db.execute_query(db.gen_state_query(numdims, numtots, types, filts))
    end = datetime.datetime.now()
    duration = end - start
    time = duration.total_seconds() * 1e3
    if time > 2000:
        logger.warning(db.get_name() + " state query time: " + str(round(time, 2)) + " ms" +
                       " parameters: " + str(types) + " " + str(filts))
    __append_results(query_name, time, category, result)
    return result


def random_state_benchmark(db: M3DB, category, reps, result):
    logger.info("Running " + category + " benchmark with " + str(reps) + " reps")
    # generate different queries
    type_options = ["S", "H"]
    S_filter_max = MAX_TAGSET_ID
    H_filter_max = MAX_NODE_ID
    for _ in range(reps):
        numdims = 3
        numtots = 3
        types, filts = __get_random_dimensions(type_options, numdims)

        # print(str(types) + "\n" + str(filts))
        state_benchmark(db, result, numdims, numtots, types, filts, db.get_name(), "Random state")
    return result


def benchmark_string_query(db: M3DB, reps, query, query_name, category, result):
    for _ in range(reps):
        start = datetime.datetime.now()
        db.execute_query(query)
        end = datetime.datetime.now()

        duration = (end - start).total_seconds() * 1e3
        __append_results(query_name, duration, category, result)
    return result


def insert_object_benchmark(db: M3DB, category, reps, result, refresh=True):
    logger.info("Running insert object benchmark in " + category + " with " + str(reps) + " reps")

    if(refresh):
        for i in range(MAX_OBJECT_ID + 1, MAX_OBJECT_ID + 1 + reps):
            start = datetime.datetime.now()
            db.insert_object(i, "BENCHMARK_OBJECT_" + str(i), 42, "BENCHMARK_OBJECT_" + str(i))
            db.refresh_object_views()
            end = datetime.datetime.now()
            duration = (end - start).total_seconds() * 1e3
            __append_results("Insert object", duration, category, result)
    else:
        for i in range(MAX_OBJECT_ID + 1, MAX_OBJECT_ID + 1 + reps):
            start = datetime.datetime.now()
            db.insert_object(i, "BENCHMARK_OBJECT_" + str(i), 42, "BENCHMARK_OBJECT_" + str(i))
            end = datetime.datetime.now()
            duration = (end - start).total_seconds() * 1e3
            __append_results("Insert object", duration, category, result)
    db.rollback()
    return result


def insert_tag_benchmark(db: M3DB, category, reps, result, refresh=True):
    logger.info("Running insert tag benchmark in " + category + " with " + str(reps) + " reps")
    if(refresh):
        for i in range(MAX_TAG_ID + 1, MAX_TAG_ID + 1 + reps):
            start = datetime.datetime.now()
            db.insert_tag(i, "BENCHMARK_TAG_" + str(i), 1, MAX_TAGSET_ID)
            db.refresh_all_views()
            end = datetime.datetime.now()
            duration = (end - start).total_seconds() * 1e3
            __append_results("Insert tag", duration, category, result)
    else:
         for i in range(MAX_TAG_ID + 1, MAX_TAG_ID + 1 + reps):
            start = datetime.datetime.now()
            db.insert_tag(i, "BENCHMARK_TAG_" + str(i), 1, MAX_TAGSET_ID)
            end = datetime.datetime.now()
            duration = (end - start).total_seconds() * 1e3
            __append_results("Insert tag", duration, category, result)
    db.rollback()
    db.delete_all_benchmark_data()
    return result


"""PostgreSQL related benchmarks """


def __psql_baseline_materialize_index_benchmark(psql: PostgresqlPC, query_name, reps, result, numdims, numtots, types,
                                                filts):
    logger.info("psql_baseline_materialize_index_benchmark : \n numdims=%i, numtots=%i, types=%s, filts=%s" % (
        numdims, numtots, types, filts))
    baseline_query = psql.gen_state_query(numdims, numtots, types, filts, baseline=True)
    materialized_view_query = psql.gen_state_query(numdims, numtots, types, filts)

    # drop materialized view indexes
    #psql.drop_materialized_indexes()

    benchmark_string_query(psql, reps, baseline_query,  query_name,"PostgreSQL Baseline", result)
    #benchmark_string_query(psql, reps, materialized_view_query,  query_name,"Materialized Views", result)
    # create materialized view indexes
    #psql.create_materialized_indexes()
    benchmark_string_query(psql, reps, materialized_view_query,  query_name,"PostgreSQL Optimized", result)
    return result


def lifelog_task_state_benchmark(db, category, reps, result, neo=False):
    numdims, numtots, types, filts = __get_lsc_lifelog_state_params()

    if neo:
        return __state_benchmark_reps(db, "Lifelog Task state", filts, numdims, numtots, reps, result, types)
    return __psql_baseline_materialize_index_benchmark(db, category, reps, result, numdims, numtots, types, filts)


def simple_state_benchmark(db, category, reps, result, neo=False):
    numdims, numtots, types, filts = __get_lsc_simple_state_params()

    if neo:
        return __state_benchmark_reps(db, "Simple state", filts, numdims, numtots, reps, result, types)
    return __psql_baseline_materialize_index_benchmark(db, category, reps, result, numdims, numtots, types, filts)


def __comp_state_benchmark(db1, db2, category, reps, result, numdims, numtots, types, filts, baseline=False):
    if (baseline):
        baseline_query = db1.gen_state_query(numdims, numtots, types, filts, baseline=True)
        benchmark_string_query(db1, reps, baseline_query, db1.get_name() + " Baseline state", category, result)
    for _ in range(reps):
        state_benchmark(db1, result, numdims, numtots, types, filts, category, db1.get_name() + " state")
        state_benchmark(db2, result, numdims, numtots, types, filts, category, db2.get_name() + " state")

    return result

def __state_benchmark_reps(db, title, filts, numdims, numtots, reps, result, types):
    for _ in range(reps):
        state_benchmark(db, result, numdims, numtots, types, filts,  db.get_name(), title)
    return result

def medium_state_benchmark(db, category, reps, result, neo=False):
    numdims, numtots, types, filts = __get_lsc_medium_state_params()

    if neo:
        return __state_benchmark_reps(db, category, filts, numdims, numtots, reps, result, types)
    return __psql_baseline_materialize_index_benchmark(db, category, reps, result, numdims, numtots, types, filts)


def complex_state_benchmark(db, category, reps, result, neo=False):
    numdims, numtots, types, filts = __get_lsc_complex_state_params()

    if neo:
        return __state_benchmark_reps(db, category, filts, numdims, numtots, reps, result, types)
    return __psql_baseline_materialize_index_benchmark(db, category, reps, result, numdims, numtots, types, filts)


def two_dimensions_state(db, category, reps, result, neo=False,vbs=False):
    numdims, numtots, types, filts = __get_vbs_2d_state_params() if vbs else __get_lsc_2d_state_params()

    if neo:
        return __state_benchmark_reps(db, category, filts, numdims, numtots, reps, result, types)

    return __psql_baseline_materialize_index_benchmark(db, category, reps, result, numdims, numtots, types, filts)


def three_dimensions_state(db, category, reps, result, neo=False, vbs=False):
    numdims, numtots, types, filts = __get_vbs_3d_state_params() if vbs else __get_lsc_3d_state_params()

    if neo:
        return __state_benchmark_reps(db, category, filts, numdims, numtots, reps, result, types)
    return __psql_baseline_materialize_index_benchmark(db, category, reps, result, numdims, numtots, types, filts)


def three_two_filters_dimensions_state(db, category, reps, result, neo=False, vbs=False):
    numdims, numtots, types, filts = __get_vbs_3d_2f_state_params() if vbs else __get_lsc_3d_2f_state_params()

    if neo:
        return __state_benchmark_reps(db, category, filts, numdims, numtots, reps, result, types)
    return __psql_baseline_materialize_index_benchmark(db, category, reps, result, numdims, numtots, types, filts)

def simple_state_comp_benchmark(db1: M3DB, db2: M3DB, category, reps, result, incl_baseline=False):
    numdims, numtots, types, filts = __get_lsc_simple_state_params()

    return __comp_state_benchmark(db1, db2, category, reps, result, numdims, numtots, types, filts, incl_baseline)


def medium_state_comp_benchmark(db1: M3DB, db2: M3DB, category, reps, result, incl_baseline=False):
    numdims, numtots, types, filts = __get_lsc_medium_state_params()

    return __comp_state_benchmark(db1, db2, category, reps, result, numdims, numtots, types, filts, incl_baseline)


def complex_state_comp_benchmark(db1: M3DB, db2: M3DB, category, reps, result, incl_baseline=False):
    numdims, numtots, types, filts = __get_lsc_complex_state_params()

    return __comp_state_benchmark(db1, db2, category, reps, result, numdims, numtots, types, filts, incl_baseline)


def comp_2d_state_benchmark(db1: M3DB, db2: M3DB, category, reps, result, incl_baseline=False):
    numdims, numtots, types, filts = __get_lsc_2d_state_params()

    return __comp_state_benchmark(db1, db2, category, reps, result, numdims, numtots, types, filts, incl_baseline)


def comp_3d_state_benchmark(db1: M3DB, db2: M3DB, category, reps, result, incl_baseline=False):
    numdims, numtots, types, filts = __get_lsc_3d_state_params()

    return __comp_state_benchmark(db1, db2, category, reps, result, numdims, numtots, types, filts, incl_baseline)


def comp_3d_2f_state_benchmark(db1: M3DB, db2: M3DB, category, reps, result, incl_baseline=False):
    numdims, numtots, types, filts = __get_lsc_3d_2f_state_params()

    return __comp_state_benchmark(db1, db2, category, reps, result, numdims, numtots, types, filts, incl_baseline)


def __get_lsc_lifelog_state_params():
    """ 2D browsing state from Figure 1,
    with children of the Dog node on
    the one axis, and timezone on the
    other axis """
    numdims = 2
    numtots = 2
    types = ["H", "S"]
    filts = [691, 14]
    return numdims, numtots, types, filts


def __get_lsc_simple_state_params():
    """ 2D browsing state with the top level of the
     entity hierarchy on one axis and location on
     the other axis """
    numdims = 2
    numtots = 2
    types = ["H", "S"]
    filts = [40, 15]
    return numdims, numtots, types, filts


def __get_lsc_medium_state_params():
    """ 3D browsing state with the top level of the
    entity hierarchy on the first axis, the
    children of the Dog node on the second axis,
    and timezone on the third axis. """
    numdims = 3
    numtots = 3
    types = ["H", "H", "S"]
    filts = [40, 691, 14]
    return numdims, numtots, types, filts


def __get_lsc_complex_state_params():
    """ 3D browsing state with the top level of the
    entity hierarchy on the first axis, the
    children of the Dog node on the second axis,
    and location on the third axis. """
    numdims = 3
    numtots = 3
    types = ["H", "H", "S"]
    filts = [40, 691, 15]
    return numdims, numtots, types, filts


def __get_lsc_2d_state_params():
    """ dog , year
     46, 183288 """
    numdims = 2
    numtots = 2
    types = ["H", "S"]
    filts = [691, 11]
    return numdims, numtots, types, filts


def __get_lsc_3d_state_params():
    """dog , location name, day of week string
    46, 127192, 183288 """
    numdims = 3
    numtots = 3
    types = ["H", "S", "H"]
    filts = [691, 15, 30]
    return numdims, numtots, types, filts


def __get_lsc_3d_2f_state_params():
    """ location, day of week string, year, dog, september
     127192, 183288, 183288, 46, """
    numdims = 3
    numtots = 5
    types = ["S", "H", "S", "H", "T"]
    filts = [15, 30, 11, 691, 13]
    return numdims, numtots, types, filts

def __get_vbs_2d_state_params():
    """ seat , category
     ,  """
    numdims = 2
    numtots = 2
    types = ["H", "S"]
    filts = [11659, 5]
    return numdims, numtots, types, filts


def __get_vbs_3d_state_params():
    """dog , category, seat
     """
    numdims = 3
    numtots = 3
    types = ["H", "S", "H"]
    filts = [13030, 5, 11659]
    return numdims, numtots, types, filts


def __get_vbs_3d_2f_state_params():
    """ segment, seat, user tag, dog, video 
     """
    numdims = 3
    numtots = 5
    types = ["S", "H", "S", "H", "T"]
    filts = [2, 11659, 4, 13030,1 ]
    return numdims, numtots, types, filts