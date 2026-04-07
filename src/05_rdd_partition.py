# 实验五：RDD分区机制与自定义分区策略
from pyspark import SparkConf, SparkContext
from pyspark import Partitioner

# 初始化SparkContext
conf = SparkConf().setAppName("RDD_Partition").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# 1. 读取产品数据，创建键值对RDD
data_path = "file:///你的本地仓库路径/data/products.txt"
product_rdd = sc.textFile(data_path)
kv_product_rdd = product_rdd.map(lambda line: (line.split(",")[0], line))

# 2. 查看默认分区数
default_partitions = kv_product_rdd.getNumPartitions()
print(f"===== 默认分区数：{default_partitions} =====")

# 3. repartition/coalesce重分区
rdd_5 = kv_product_rdd.repartition(5)
rdd_2 = rdd_5.coalesce(2)
print(f"repartition后5个分区：{rdd_5.getNumPartitions()}")
print(f"coalesce后2个分区：{rdd_2.getNumPartitions()}")

# 4. 自定义分区器（按产品类别分区）
class CustomPartitioner(Partitioner):
    def __init__(self, numPartitions):
        self.numPartitions = numPartitions
    def getPartition(self, key):
        return hash(key) % self.numPartitions
    def __eq__(self, other):
        return isinstance(other, CustomPartitioner) and self.numPartitions == other.numPartitions

# 5. 应用自定义分区器
partitioned_rdd = kv_product_rdd.partitionBy(3, CustomPartitioner(3))
print(f"\n===== 自定义分区后分区数：{partitioned_rdd.getNumPartitions()} =====")

# 6. glom查看分区内容
partition_content = partitioned_rdd.glom().collect()
for i, content in enumerate(partition_content):
    print(f"分区{i}的内容（前3条）：{content[:3]}")

# 关闭SparkContext
sc.stop()
print("\n✅ 实验五执行完成！")
