# 实验五：RDD分区机制与自定义分区策略
from pyspark import SparkConf, SparkContext
from pyspark import Partitioner

# 1. 初始化SparkContext
conf = SparkConf().setAppName("RDD_Partition").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# 2. 读取产品数据，创建键值对RDD（产品类别为Key，整行数据为Value）
data_path = "你的本地data目录路径/products.txt"
product_rdd = sc.textFile(data_path)
kv_product_rdd = product_rdd.map(lambda line: (line.split(",")[0], line))

# 3. 查看默认分区数（与CPU核心数相关）
default_partitions = kv_product_rdd.getNumPartitions()
print(f"===== 默认分区数：{default_partitions} =====")

# 4. repartition/coalesce重分区对比
print("\n===== 重分区操作 =====")
rdd_5 = kv_product_rdd.repartition(5)  # 增加分区到5（触发Shuffle）
rdd_2 = rdd_5.coalesce(2)  # 减少分区到2（默认不触发Shuffle）
print(f"repartition后分区数：{rdd_5.getNumPartitions()}")
print(f"coalesce后分区数：{rdd_2.getNumPartitions()}")
print("结论：repartition可增减分区（Shuffle），coalesce仅减分区（高效）")

# 5. 自定义分区器（按产品类别Hash分区，确保同类数据在同一分区）
class CustomPartitioner(Partitioner):
    def __init__(self, numPartitions):
        self.numPartitions = numPartitions  # 分区数

    def getPartition(self, key):
        # 自定义分区逻辑：Key的Hash值取模分区数
        return hash(key) % self.numPartitions

    def __eq__(self, other):
        # 用于比较分区器是否相同（Spark内部使用）
        return isinstance(other, CustomPartitioner) and self.numPartitions == other.numPartitions

# 6. 应用自定义分区器（3个分区）
partitioned_rdd = kv_product_rdd.partitionBy(3, CustomPartitioner(3))
print(f"\n===== 自定义分区后分区数：{partitioned_rdd.getNumPartitions()} =====")

# 7. glom查看分区内容，验证分区效果
print("\n===== 各分区内容（前3条） =====")
partition_content = partitioned_rdd.glom().collect()  # glom()将分区元素转为列表
for i, content in enumerate(partition_content):
    print(f"分区{i}：{content[:3]}")  # 打印每个分区前3条数据

# 关闭SparkContext
sc.stop()
print("\n✅ 实验五（RDD分区机制）执行完成！")
