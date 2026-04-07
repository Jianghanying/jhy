# 实验六：RDD数据的持久化与分布式存储
from pyspark import SparkConf, SparkContext
from pyspark import StorageLevel
import time
import os
import shutil

# 1. 初始化SparkContext
conf = SparkConf().setAppName("RDD_Persist_Save").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# 2. 构建单词统计RDD（为存储做准备）
data_path = "你的本地data目录路径/sentences.txt"
sent_rdd = sc.textFile(data_path)
word_count_rdd = sent_rdd.flatMap(lambda line: line.strip().split(" ")) \
                         .map(lambda word: word.lower()) \
                         .filter(lambda word: len(word) >= 3) \
                         .map(lambda word: (word, 1)) \
                         .reduceByKey(lambda a, b: a + b)

print("===== 单词统计结果前10条 =====")
print(word_count_rdd.take(10))

# 3. saveAsTextFile保存RDD（分区数=输出文件数）
# 定义输出路径（替换为你的本地输出目录）
path1 = "你的本地输出目录/wordcount_default"
path2 = "你的本地输出目录/wordcount_5parts"

# 先删除旧路径（避免Spark报错）
for path in [path1, path2]:
    if os.path.exists(path):
        shutil.rmtree(path)

# 保存RDD（默认分区+5个分区）
word_count_rdd.saveAsTextFile(path1)
word_count_rdd.repartition(5).saveAsTextFile(path2)
print("\n===== RDD保存完成 =====")
print(f"默认分区保存路径：{path1}")
print(f"5分区保存路径：{path2}")
print("结论：输出文件数=RDD分区数，每个分区对应一个part-xxxxx文件")

# 4. 验证RDD持久化（cache/persist）性能差异
print("\n===== 持久化性能对比 =====")
# 读取大文件构建RDD（使用articles.txt模拟大数据场景）
big_data_path = "你的本地data目录路径/articles.txt"
big_rdd = sc.textFile(big_data_path).map(lambda line: line.upper())

# 未缓存：两次count均重复计算
start1 = time.time()
big_rdd.count()
t1 = time.time() - start1
start2 = time.time()
big_rdd.count()
t2 = time.time() - start2

# 缓存：第一次count触发计算并缓存，第二次直接读取缓存
big_rdd.cache()  # 默认存储级别：MEMORY_ONLY（仅内存）
big_rdd.count()  # 触发计算+缓存
start3 = time.time()
big_rdd.count()
t3 = time.time() - start3

# 打印耗时对比
print(f"未缓存-第一次count：{t1:.2f}s")
print(f"未缓存-第二次count：{t2:.2f}s")
print(f"缓存后-count：{t3:.2f}s")
print("结论：缓存后耗时大幅降低，避免重复计算（迭代算法/多次查询必备）")

# 关闭SparkContext
sc.stop()
print("\n✅ 实验六（RDD持久化与存储）执行完成！")
