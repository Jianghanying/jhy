# 实验二：RDD操作算子的分类与惰性求值验证
from pyspark import SparkConf, SparkContext
import time

# 1. 初始化SparkContext
conf = SparkConf().setAppName("RDD_Operator_Type").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# 2. 创建测试RDD（从Python集合并行化创建）
num_rdd = sc.parallelize([1, 2, 3, 4, 5])

# 3. 测试Transformation算子（map）：验证惰性求值
print("===== 测试Transformation算子（map） =====")
start_time = time.time()
# map算子：每个元素翻倍（仅记录操作，不触发计算）
mapped_rdd = num_rdd.map(lambda x: x * 2)
end_time = time.time()
print(f"map转换执行耗时：{end_time - start_time:.6f}s")
print("结论：Transformation算子仅记录操作，未触发实际计算（惰性求值）")

# 4. 测试Action算子（count）：触发实际计算
print("\n===== 测试Action算子（count） =====")
start_time = time.time()
# count算子：统计元素个数（触发全流程计算）
count_result = mapped_rdd.count()
end_time = time.time()
print(f"count执行耗时：{end_time - start_time:.6f}s")
print(f"mapped_rdd元素个数：{count_result}")
print(f"mapped_rdd计算结果：{mapped_rdd.collect()}")  # 小数据集可使用collect
print("结论：Action算子触发实际计算，返回结果给Driver")

# 5. 查看RDD血缘关系（Lineage）：验证操作记录
print("\n===== RDD血缘关系（Lineage） =====")
# toDebugString()返回字节流，需解码为字符串
print(mapped_rdd.toDebugString().decode("utf-8"))
print("结论：Spark记录RDD的所有转换步骤，为容错提供支撑")

# 6. 验证未缓存RDD的重复计算
print("\n===== 验证未缓存RDD的重复计算 =====")
start1 = time.time()
mapped_rdd.take(3)  # 第一次触发计算
t1 = time.time() - start1
start2 = time.time()
mapped_rdd.take(3)  # 第二次重复计算
t2 = time.time() - start2
print(f"第一次take耗时：{t1:.6f}s")
print(f"第二次take耗时：{t2:.6f}s")
print("结论：未缓存的RDD每次调用Action都会重复计算，效率低下")

# 关闭SparkContext
sc.stop()
print("\n✅ 实验二（RDD算子分类与惰性求值）执行完成！")
