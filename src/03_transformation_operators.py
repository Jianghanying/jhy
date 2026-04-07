# 实验三：常用Transformation算子的应用
from pyspark import SparkConf, SparkContext
import string

# 初始化SparkContext
conf = SparkConf().setAppName("Transformation_Operators").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# 1. 读取句子数据
data_path = "file:///你的本地仓库路径/data/sentences.txt"
sent_rdd = sc.textFile(data_path)

# 2. flatMap分词（一对多映射）
word_rdd = sent_rdd.flatMap(lambda line: line.strip().split(" "))
print("===== flatMap分词后前10个单词 =====")
print(word_rdd.take(10))

# 3. map转小写
lower_word_rdd = word_rdd.map(lambda word: word.lower().strip(string.punctuation))
print("\n===== 转小写后前10个单词 =====")
print(lower_word_rdd.take(10))

# 4. filter过滤短单词（长度<3）
filter_word_rdd = lower_word_rdd.filter(lambda word: len(word) >= 3 and word != "")
print("\n===== 过滤短单词后前10个单词 =====")
print(filter_word_rdd.take(10))

# 5. distinct去重
unique_word_rdd = filter_word_rdd.distinct()
print("\n===== 去重后前10个单词 =====")
print(unique_word_rdd.take(10))

# 6. 统计各阶段数据量
print(f"\n===== 数据量统计 =====")
print(f"分词后总单词数：{word_rdd.count()}")
print(f"过滤后单词数：{filter_word_rdd.count()}")
print(f"去重后唯一单词数：{unique_word_rdd.count()}")

# 关闭SparkContext
sc.stop()
print("\n✅ 实验三执行完成！")
