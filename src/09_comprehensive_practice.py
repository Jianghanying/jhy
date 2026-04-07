# 实验九：基于RDD模型的分布式编程综合实战
from pyspark import SparkConf, SparkContext
import string

# 1. 初始化SparkContext
conf = SparkConf().setAppName("Comprehensive_Practice").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# 实战1：日志数据去重与统计
print("===== 实战1：日志数据去重与统计 =====")
log_path = "你的本地data目录路径/logs.txt"
log_rdd = sc.textFile(log_path)
log_result = log_rdd.filter(lambda line: line.strip() != "") \
                   .distinct()  # 去重
                   .map(lambda line: (line.split(",")[1], 1))  # 提取日志类型
                   .reduceByKey(lambda a, b: a + b)  # 统计次数
                   .sortBy(lambda x: x[1], ascending=False)  # 按次数降序
print("日志类型统计前5名：", log_result.take(5))

# 实战2：员工-部门多数据集关联分析
print("\n===== 实战2：员工-部门关联分析 =====")
# 读取部门数据
dept_path = "你的本地data目录路径/departments.txt"
dept_rdd = sc.textFile(dept_path).map(lambda line: (line.split(",")[0], line.split(",")[1]))
# 读取员工数据（复用实验八的kv_emp_rdd）
emp_path = "你的本地data目录路径/employees.txt"
emp_rdd = sc.textFile(emp_path)
clean_emp_rdd = emp_rdd.filter(lambda line: len(line.strip().split(",")) == 3 and line.strip().split(",")[2].isdigit())
kv_emp_rdd = clean_emp_rdd.map(lambda line: (line.split(",")[0], float(line.split(",")[2])))

# 员工数据聚合（部门ID：(人数, 工资总和)）
emp_agg = kv_emp_rdd.combineByKey(
    lambda s: (1, s),
    lambda acc, s: (acc[0]+1, acc[1]+s),
    lambda a1, a2: (a1[0]+a2[0], a1[1]+a2[1])
).mapValues(lambda x: (x[0], round(x[1]/x[0], 2)))  # 计算平均工资

# 关联部门和员工聚合数据，按平均工资降序
join_rdd = dept_rdd.join(emp_agg)
# 格式化结果：(部门名称, 员工数, 平均工资)
formatted_join = join_rdd.map(lambda x: (x[1][1][0], x[1][0], x[1][1][1])).sortBy(lambda x: x[2], ascending=False)

print("部门关联分析（按平均工资降序）：")
for res in formatted_join.collect():
    print(f"员工数：{res[0]} | 部门：{res[1]} | 平均工资：{res[2]}")

# 实战3：进阶单词计数（去标点+过滤停用词）
print("\n===== 实战3：进阶单词计数 =====")
article_path = "你的本地data目录路径/articles.txt"
article_rdd = sc.textFile(article_path)
# 定义停用词（无意义单词）
stop_words = {"a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with"}

word_freq = article_rdd.flatMap(lambda line: line.strip().split(" ")) \
                       .map(lambda word: word.lower().translate(str.maketrans("", "", string.punctuation))) \
                       .filter(lambda word: word not in stop_words and len(word) >= 2) \
                       .map(lambda word: (word, 1)) \
                       .reduceByKey(lambda a, b: a + b) \
                       .sortBy(lambda x: x[1], ascending=False)

print("Top20高频词：", word_freq.take(20))

# 实战4：倒排索引构建（单词→文档ID列表）
print("\n===== 实战4：倒排索引 =====")
doc_path = "你的本地data目录路径/documents.txt"
doc_rdd = sc.textFile(doc_path)
invert_index = doc_rdd.filter(lambda line: line.strip() != "") \
                      .flatMap(lambda line: [(word.lower(), line.split(",")[0]) for word in line.split(",")[1].strip().split(" ")]) \
                      .groupByKey() \
                      .mapValues(lambda x: list(set(x)))  # 去重文档ID

print("倒排索引前10个单词：")
for res in invert_index.take(10):
    print(f"{res[0]} → {res[1]}")

# 关闭SparkContext
sc.stop()
print("\n✅ 实验九（综合实战）执行完成！")
