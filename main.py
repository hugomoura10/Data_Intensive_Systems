import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,concat_ws, collect_list, lit,split, size, avg, udf, row_number, when, date_format, length
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import max as spark_max

from datasketch import MinHash, MinHashLSH
import numpy as np 
from numpy import average

import shutil
import os
import resource
import time
import psutil
import matplotlib.pyplot as plt

def get_memory_usage(): 
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024

def get_cpu_usage(): 
    return psutil.cpu_percent(interval=None)

def get_performance(func1,func2, vals):
    results = []

    for k in vals:
        start_time = time.time()
        start_mem = get_memory_usage()
        start_cpu = get_cpu_usage()

        replacement_candidates, minhashes = func1(df_grouped, k, 0.98)
        new_process_dictionary = func2(replacement_candidates)
        
        end_time = time.time()
        end_mem = get_memory_usage()
        end_cpu = get_cpu_usage()

        duration = end_time - start_time
        mem_used = end_mem - start_mem

        results.append({
            'k': k,
            'time_seconds': duration,
            'memory_mb': mem_used,
            'unique_processes': len(new_process_dictionary),
            'cpu': end_cpu
        })
    return results

def plot_results(results):
    k_values = [result['k'] for result in results]
    time_seconds = [result['time_seconds'] for result in results]
    cpu_percentages = [result['cpu'] for result in results]
    fig, ax1 = plt.subplots(figsize=(10, 6))

    ax1.set_xlabel('k values')
    ax1.set_ylabel('Time (seconds)', color='tab:blue')
    ax1.plot(k_values, time_seconds, marker='o', color='tab:blue', label='Time')
    ax1.tick_params(axis='y', labelcolor='tab:blue')

    ax2 = ax1.twinx()
    ax2.set_ylabel('CPU Usage (%)', color='tab:red')

    ax2.plot(k_values, cpu_percentages, marker='^', color='tab:red', linestyle='--', label='CPU Usage')
    ax2.tick_params(axis='y', labelcolor='tab:red')

    plt.title('Performance Metrics for Different k Values')
    fig.legend(loc='upper left')
    plt.tight_layout()
    plt.grid(True)
    plt.show()

def plot_performances(results):
    k_values = [result['k'] for result in results]
    time_seconds = [result['time_seconds'] for result in results]
    cpu_percentages = [result['cpu'] for result in results]

    performance_metric = [time_seconds[i] * cpu_percentages[i] for i in range(len(results))]

    plt.figure(figsize=(10, 6))
    plt.plot(k_values, performance_metric, marker='o', linestyle='-', color='purple', label='Time * CPU')
    plt.xlabel('k values')
    plt.ylabel('Performance Metric (Time * CPU)')
    plt.title('Combined Metric of Time and CPU Usage vs. k Values')
    plt.xticks(k_values)
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    plt.show()

def distinct_elements_in_order(lst):
    seen = set()
    result = []
    for item in lst:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result

def shingle(text, k=7):
    shingle_set = []
    for i in range(len(text)-k +1):
        shingle_set.append(text[i:i+k])
    return list(set(shingle_set))

def jaccard_similarity(list1, list2):   
    return len(set(list1).intersection(set(list2))) / len(set(list1).union(set(list2)))

def minhash_lsh(df, k_shingle, threshold):

    lsh = MinHashLSH(threshold=threshold, num_perm=128)
    minhashes = {}

    for features in df.collect():
        shingles = shingle(features["features"], k_shingle)
        m = MinHash(num_perm=128)
        for shingle_item in shingles:
            m.update(shingle_item.encode("utf8"))
        minhashes[int(features["user_id"])] = m
        lsh.insert(int(features["user_id"]), m)

    replacement_candidates = {}
    for key in lsh.keys: 
        replacement_candidates[key] = lsh.query(minhashes[key]) 

    return replacement_candidates,minhashes


def remove_dups(graph):
    keys_to_remove = []
    for key, values in graph.items():
        if key in values:
            values.remove(key)
        if not values:
            keys_to_remove.append(key)
        else:
            graph[key] = values  
    
    for key in keys_to_remove:
        graph.pop(key)

    return graph

def bucketing(replacement_candidates):
    visited_processes = set()
    new_process_dictionary = {}

    def bfs(start):
        queue = [start]
        bucket = []
        while queue:
            current = queue.pop(0)
            if current not in visited_processes:
                visited_processes.add(current)
                bucket.append(current)
                if current in replacement_candidates:
                    for neighbor in replacement_candidates[current]:
                        if neighbor not in visited_processes:
                            queue.append(neighbor)
        return bucket

    for key in replacement_candidates.keys():
        if key not in visited_processes:
            bucket = bfs(key)
            if bucket:
                new_process_dictionary[key] = sorted(bucket)

    return new_process_dictionary

def get_case(caseID,data):
    data1 = data.filter(data.user_id.isin([caseID]))
    data1.show()

def compare_cases(case1,case2,data):
    data1 = data.filter(data.user_id.isin([case1]))
    data2 = data.filter(data.user_id.isin([case2]))
    desired_column_list1 = data1.select("to").rdd.flatMap(lambda x: x).collect()
    desired_column_list2 = data2.select("to").rdd.flatMap(lambda x: x).collect()

    common_elements = np.intersect1d(desired_column_list1, desired_column_list2)
    union_elements = np.union1d(desired_column_list1, desired_column_list2)
    print(len(common_elements)/len(union_elements))

def get_traces(user_id,df):
    result = df.filter(col("user_id") == user_id).select("features").collect()
    if result:
        return result[0]["features"]
    else:
        return None

def get_shingles(user_id,df):
    result = df.filter(col("user_id") == user_id).select("shingles").collect()
    if result:
        return result[0]["shingles"]
    else:
        return None

def experiment(data,k_value,threshold=0,approach =1):
    
    def shingle(text, k=k_value):
        shingle_set = []
        for i in range(len(text)-k +1):
            shingle_set.append(text[i:i+k])
        return list(set(shingle_set))
    if approach == 1:
        df_filtered = data.filter(data.type.isin(['Req']))
        
    else:
        df_filtered = data.filter((col("from") == "S0") & (~col("to").contains("null")) & (~col("to").contains("_")))
    
    df_grouped = df_filtered.groupBy("user_id").agg(concat_ws("",collect_list("to")).alias("features"))
    shingles_udf = udf(shingle, ArrayType(StringType()))
    df_shingles = df_filtered.groupBy("user_id").agg(concat_ws("", collect_list("to")).alias("trace")) \
        .withColumn("shingles", shingles_udf(col("trace"))) \
        .select("user_id", "shingles")
    


    average_length = df_grouped.select(avg(length(col('features')))).collect()[0][0]
    average_shingles = df_shingles.withColumn("list_length", size(col("shingles"))) \
                        .agg(avg("list_length").alias("average_list_length")).collect()[0][0]

    print('average trace length:',average_length)

    ############################################################
    print('average trace # shingles:',average_shingles)
    print(f"Initial number of cases: {df_grouped.count()}")
    if threshold==0:
        threshold = (int(average_shingles)-1)/int(average_shingles)
    print('threshold:', threshold)
    ans = minhash_lsh(df_grouped,7,threshold)
    replacement_candidates, minhash_dic = ans[0],ans[1]
    new_process_dictionary= bucketing(replacement_candidates)
    print(f"Number of unique processes after merging them with {threshold} threshold using 7-shingles: {len(new_process_dictionary)}")

    ############################################################
    sims = get_averege_jaccard_sim(replacement_candidates, minhash_dic,get=False)

    if len(set(value for key,values in sims.items() for value in values if value != 1.0)) != 0:
        ans = min(set(value for key,values in sims.items() for value in values if value != 1.0))
        final_values = []
        for key,values in sims.items():
            for value in values:
                if value == ans:
                    final_values.append(key)

        dissimilar = set(final_values)
        new_sims = []
        for key in dissimilar:
            for value in replacement_candidates[key]:
                new_sims.append((key,value,jaccard_similarity(get_shingles(value,df_shingles),get_shingles(key,df_shingles))))
        investigate = [case for case in new_sims if case[-1]!=1.0]
        done_cases = []
        for case in investigate:
            if case[0] not in done_cases and case[1] not in done_cases:
                print(f'######################### {case[0]} vs {case[1]} ################################')
                print('jaccard similarity:',jaccard_similarity(get_shingles(case[0],df_shingles), get_shingles(case[1],df_shingles)))
                print(case[0],':',get_traces(case[0],df_grouped))
                print(case[1],':',get_traces(case[1],df_grouped))
                done_cases.append(case[0])
                done_cases.append(case[1])
                print('#######################################################################')

    else:
        print('all processes have approximate jaccard sim = 1')



    
def write_output1(df, file_name):
    output_dir = 'Output'
    os.makedirs(output_dir, exist_ok=True)
    df = df.withColumn("timestamp", date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSS"))
    
    temp_output_dir = 'temp/temp_output'
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_output_dir)
    part_file = [f for f in os.listdir(temp_output_dir) if f.startswith("part-")][0]
    shutil.move(os.path.join(temp_output_dir, part_file), os.path.join(output_dir, file_name))
    shutil.rmtree(temp_output_dir)

def write_output2(df, groups):
    os.makedirs('Output', exist_ok=True)

    with open('Output/part2Observations.txt', 'w') as file:
        for group_name, user_ids in groups.items():
            file.write(f"Group: {set(user_ids)}\n")
            
            for user_id in user_ids:
                user_df = df.filter(col("user_id") == user_id)
                file.write(f"{user_id}:\n")
                for row in user_df.collect():
                    file.write(f"< {row['from']}, {row['to']}, {row['timestamp']}, {row['type']}, {row['user_id']} >\n")
                
                file.write("\n")

def output(dataset, k, threshold,p1=True):
    spark = SparkSession.builder.appName("OutputUserIDs").getOrCreate()
    data = spark.read.csv(dataset, header=True, inferSchema=True)
    if p1 == True:
        df_filtered = data.filter(data.type.isin(['Req']))
        df_grouped = df_filtered.groupBy("user_id").agg(concat_ws("", collect_list("to")).alias("features"))
        
    
    else:
        df_filtered = data.filter((col("from") == "S0") & (~col("to").contains("null")) & (~col("to").contains("_")))
        df_grouped = df_filtered.groupBy("user_id").agg(concat_ws("",collect_list("to")).alias("features"))
    
    max_user_id = df_grouped.select(spark_max("user_id")).collect()[0][0]
    df_grouped.show()
    
        
    replacement_candidates = minhash_lsh(df_grouped, k, threshold)
    new_process_dictionary = bucketing(replacement_candidates[0])    

    user_ids_to_change = []#[key for key in new_process_dictionary.keys()]#[key for key, values in new_process_dictionary.items() if len(values) > 1 or ]
    for key, values in new_process_dictionary.items():
        if len(values) >1:
            user_ids_to_change.append(key)
        elif len(values) == 1 and values[0]!= key:
            user_ids_to_change.append(key)

    user_ids_to_delete = []
    for id in user_ids_to_change:
        for case in new_process_dictionary[id]:
            if case not in user_ids_to_change:
                user_ids_to_delete.append(case)
    
    distinct_user_ids_df = df_grouped.select("user_id").distinct()
    distinct_user_ids = set([row.user_id for row in distinct_user_ids_df.collect()])

    user_ids_to_maintain = distinct_user_ids-set(user_ids_to_delete)-set(user_ids_to_change)
    maintain_df = data.filter(col("user_id").isin(list(user_ids_to_maintain)))  
    
    change_df = data.filter(col("user_id").isin(user_ids_to_change))

    distinct_user_ids_to_change = change_df.select("user_id").distinct()
    window_spec = Window.orderBy("user_id")
    user_id_mapping = distinct_user_ids_to_change.withColumn("new_user_id", row_number().over(window_spec) + max_user_id )
    df_with_new_ids = data.join(user_id_mapping, on="user_id", how="right")
    df_with_new_ids = df_with_new_ids.withColumn("user_id",
                                                 when(col("new_user_id").isNotNull(), col("new_user_id"))
                                                 .otherwise(col("user_id"))) \
                                     .drop("new_user_id")
    columns = df_with_new_ids.columns
    columns.remove('user_id')
    new_column_order = columns + ['user_id']
    df_with_new_ids = df_with_new_ids.select(new_column_order)

    final_df = df_with_new_ids.union(maintain_df)
    if p1== True:
        write_output1(final_df, 'part1Output.txt')
    write_output2(data, new_process_dictionary)

def get_averege_jaccard_sim(final_buckets, minhashes,get = True):
    sims = {}
    for key, value in final_buckets.items():
        for user_id_1 in final_buckets[key]:
            for user_id_2 in final_buckets[key]:
                if user_id_1 != user_id_2:
                    sig_1 = minhashes[int(user_id_1)]
                    sig_2 = minhashes[int(user_id_2)]
                    sim = MinHash.jaccard(sig_1, sig_2)
                    if key not in sims:
                        sims[key] = [sim]
                    else:
                        sims[key].append(sim)
    total_sum = 0
    total_count = 0
    sims = dict(sorted(sims.items()))
    if get == True:
        for key, value in sims.items():
            avg_sim = average(value)
            print(key, avg_sim)
            total_sum += sum(value)
            total_count += len(value)
        
        overall_average = total_sum / total_count if total_count != 0 else 0
        print("Overall Average Jaccard Similarity:", overall_average)

    return sims