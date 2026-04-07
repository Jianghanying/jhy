# 实验四：常用Action算子与结果获取
from pyspark import SparkConf, SparkContext

# 1. 初始化SparkContext
conf = SparkConf().setAppName("Action_Operators").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# 2. 创建测试RDD（1-100的数字）
num_rdd = sc.parallelize(range(1, 101))

# 3. 基础Action算子：count/take/first
print("===== 基础Action算子 =====")
total = num_rdd.count()  # 统计总元素数
top5 = num_rdd.take(5)   # 安全预览前5个元素
first_ele = num_rdd.first()  # 获取第一个元素
print(f"RDD总元素数：{total}")
print(f"前5个元素：{top5}")
print(f"第一个元素：{first_ele}")

# 4. reduce算子：聚合计算（求和、求积）
print("\n===== reduce聚合算子 =====")
sum_total = num_rdd.reduce(lambda a, b: a + b)  # 1-100求和
product_10 = num_rdd.filter(lambda x: x <= 10).reduce(lambda a, b: a * b)  # 1-10求积
print(f"1-100的和：{sum_total}（理论值：5050）")
print(f"1-10的积：{product_10}（理论值：3628800）")

# 5. takeOrdered算子：排序取数（TopN/MinN）
print("\n===== takeOrdered排序取数 =====")
min5 = num_rdd.takeOrdered(5)  # 默认升序，取最小5个
max5 = num_rdd.takeOrdered(5, key=lambda x: -x)  # 自定义key，取最大5个
print(f"最小的5个元素：{min5}")
print(f"最大的5个元素：{max5}")

# 6. collect算子：小数据集获取所有元素（大数据集慎用）
print("\n===== collect算子（小数据集） =====")
all_ele = num_rdd.collect()
print(f"RDD所有元素（前10个）：{all_ele[:10]}")
print("警告：大数据集使用collect可能导致Driver内存溢出（OOM）")

# 关闭SparkContext
sc.stop()
print("\n✅ 实验四（常用Action算子）执行完成！")
