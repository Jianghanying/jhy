# 实验三：常用Transformation算子的综合应用
from pyspark import SparkConf, SparkContext

# 1. 初始化SparkContext
conf = SparkConf().setAppName("Transformation_Operators").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# 2. 读取句子数据（替换为你的本地data目录路径）
data_path = "你的本地data目录路径/sentences.txt"
sent_rdd = sc.textFile(data_path)

# 3. flatMap算子：分词（一对多映射+展平）
print("===== 1. flatMap分词（一对多映射） =====")
word_rdd = sent_rdd.flatMap(lambda line: line.strip().split(" "))
print("分词后前10个单词：", word_rdd.take(10))

# 4. map算子：单词转小写（一对一映射）
print("\n===== 2. map转小写（一对一映射） =====")
lower_word_rdd = word_rdd.map(lambda word: word.lower())
print("转小写后前10个单词：", lower_word_rdd.take(10))

# 5. filter算子：过滤短单词（长度<3）
print("\n===== 3. filter过滤短单词 =====")
filter_word_rdd = lower_word_rdd.filter(lambda word: len(word) >= 3)
print("过滤后前10个单词：", filter_word_rdd.take(10))

# 6. distinct算子：去除重复单词（需Shuffle，开销较大）
print("\n===== 4. distinct去重 =====")
unique_word_rdd = filter_word_rdd.distinct()
print("去重后前10个单词：", unique_word_rdd.take(10))

# 7. 统计各阶段数据量，验证算子效果
print("\n===== 各阶段数据量统计 =====")
print(f"分词后总单词数：{word_rdd.count()}")
print(f"过滤后单词数：{filter_word_rdd.count()}")
print(f"去重后唯一单词数：{unique_word_rdd.count()}")

# 关闭SparkContext
sc.stop()
print("\n✅ 实验三（常用Transformation算子）执行完成！")
