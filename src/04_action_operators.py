# 实验四：常用Action算子与结果获取
from pyspark import SparkConf, SparkContext

# 初始化SparkContext
conf = SparkConf().setAppName("Action_Operators").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# 1. 创建测试RDD（1-100）
num_rdd = sc.parallelize(range(1, 101))

# 2. count/take/first
total = num_rdd.count()
top5 = num_rdd.take(5)
first_ele = num_rdd.first()
print("===== 基础Action算子 =====")
print(f"RDD总元素数：{total}")
print(f"前5个元素：{top5}")
print(f"第一个元素：{first_ele}")

# 3. reduce聚合
sum_total = num_rdd.reduce(lambda a, b: a + b)
product_10 = num_rdd.filter(lambda x: x <= 10).reduce(lambda a, b: a * b)
print("\n===== reduce聚合 =====")
print(f"1-100的和：{sum_total}（理论值5050）")
print(f"1-10的积：{product_10}（理论值3628800）")

# 4. takeOrdered排序取数
min5 = num_rdd.takeOrdered(5)
max5 = num_rdd.takeOrdered(5, key=lambda x: -x)
print("\n===== takeOrdered =====")
print(f"最小的5个元素：{min5}")
print(f"最大的5个元素：{max5}")

# 5. collect（小数据集）
all_ele = num_rdd.collect()
print("\n===== collect（前10个元素） =====")
print(f"RDD所有元素前10个：{all_ele[:10]}")

# 关闭SparkContext
sc.stop()
print("\n✅ 实验四执行完成！")
