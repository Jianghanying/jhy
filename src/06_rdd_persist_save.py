# 实验六：RDD数据的持久化与分布式存储
from pyspark import SparkConf, SparkContext
from pyspark import StorageLevel
import time
import string
import os
import shutil

# 初始化SparkContext
conf = SparkConf().setAppName("RDD_Persist_Save").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# 1. 单词统计RDD
data_path = "file:///你的本地仓库路径/data/sentences.txt"
sent_rdd = sc.textFile(data_path)
word_count_rdd = sent_rdd.flatMap(lambda line: line.strip().split(" ")) \
                         .map(lambda word: word.lower().strip(string.punctuation)) \
                         .filter(lambda word: len(word) >= 3) \
                         .map(lambda word: (word, 1)) \
                         .reduceByKey(lambda a, b: a + b)

print("===== 单词统计结果前10条 =====")
print(word_count_rdd.take(10))

# 2. saveAsTextFile保存（分区数=输出文件数）
path1 = "/你的本地路径/output/wordcount_default"
path2 = "/你的本地路径/output/wordcount_5parts"
for path in [path1, path2]:
    if os.path.exists(path):
        shutil.rmtree(path)

word_count_rdd.saveAsTextFile(f"file://{path1}")
word_count_rdd.repartition(5).saveAsTextFile(f"file://{path2}")
print("\n===== RDD保存完成 =====")
print(f"默认分区保存至：{path1}")
print(f"5分区保存至：{path2}")

# 3. 持久化对比
big_rdd = sc.textFile("file:///你的本地仓库路径/data/articles.txt").map(lambda line: line.upper())
print("\n===== 持久化性能对比 =====")
# 未缓存
start1 = time.time()
big_rdd.count()
t1 = time.time() - start1
start2 = time.time()
big_rdd.count()
t2 = time.time() - start2
print(f"未缓存-第一次count：{t1:.2f}s")
print(f"未缓存-第二次count：{t2:.2f}s")

# 缓存
big_rdd.cache()
big_rdd.count()  # 触发缓存
start3 = time.time()
big_rdd.count()
t3 = time.time() - start3
print(f"缓存后-count：{t3:.2f}s（耗时大幅降低）")

# 关闭SparkContext
sc.stop()
print("\n✅ 实验六执行完成！")
