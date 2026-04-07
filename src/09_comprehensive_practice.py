# 实验九：基于RDD模型的分布式编程综合实战
from pyspark import SparkConf, SparkContext
import string

# 初始化SparkContext
conf = SparkConf().setAppName("Comprehensive_Practice").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# 实战1：日志数据去重与统计
print("===== 实战1：日志数据去重与统计 =====")
log_path = "file:///你的本地仓库路径/data/logs.txt"
log_rdd = sc.textFile(log_path)
log_result = log_rdd.filter(lambda line: line.strip() != "") \
                   .distinct() \
                   .map(lambda line: (line.split(",")[1], 1)) \
                   .reduceByKey(lambda a, b: a + b) \
                   .sortBy(lambda x: x[1], ascending=False)
print("日志类型统计前5：", log_result.take(5))

# 实战2：员工-部门多数据集关联
print("\n===== 实战2：员工-部门关联分析 =====")
dept_path = "file:///你的本地仓库路径/data/departments.txt"
dept_rdd = sc.textFile(dept_path).map(lambda line: (line.split(",")[0], line.split(",")[1]))
# 关联+聚合
join_result = kv_emp_rdd.combineByKey(
    lambda s: (1, s),
    lambda acc, s: (acc[0]+1, acc[1]+s),
    lambda a1, a2: (a1[0]+a2[0], a1[1]+a2[1])
).mapValues(lambda x: (x[0], round(x[1]/x[0], 2))).join(dept_rdd)
# 格式化排序
formatted_join = join_result.map(lambda x: (x[1][1], x[1][0][0], x[1][0][1])).sortBy(lambda x: x[2], ascending=False)
print("部门关联分析（按平均工资降序）：")
for res in formatted_join.collect():
    print(f"部门：{res[0]} | 员工数：{res[1]} | 平均工资：{res[2]}")

# 实战3：进阶单词计数（去标点+过滤停用词）
print("\n===== 实战3：进阶单词计数 =====")
article_path = "file:///你的本地仓库路径/data/articles.txt"
article_rdd = sc.textFile(article_path)
stop_words = {"a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with"}
word_freq = article_rdd.flatMap(lambda line: line.strip().split(" ")) \
                       .map(lambda word: word.lower().translate(str.maketrans("", "", string.punctuation))) \
                       .filter(lambda word: word not in stop_words and len(word) >= 2) \
                       .map(lambda word: (word, 1)) \
                       .reduceByKey(lambda a, b: a + b) \
                       .sortBy(lambda x: x[1], ascending=False)
print("Top20高频词：", word_freq.take(20))

# 实战4：倒排索引构建
print("\n===== 实战4：倒排索引 =====")
doc_path = "file:///你的本地仓库路径/data/documents.txt"
doc_rdd = sc.textFile(doc_path)
invert_index = doc_rdd.filter(lambda line: line.strip() != "") \
                      .flatMap(lambda line: [(word.lower(), line.split(",")[0]) for word in line.split(",")[1].strip().split(" ")]) \
                      .groupByKey() \
                      .mapValues(lambda x: list(set(x)))
print("倒排索引前10个单词：")
for res in invert_index.take(10):
    print(f"{res[0]} -> {res[1]}")

# 关闭SparkContext
sc.stop()
print("\n✅ 实验九执行完成！")
