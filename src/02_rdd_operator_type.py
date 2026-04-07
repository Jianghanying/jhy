# 实验二：RDD操作算子的分类与惰性求值
from pyspark import SparkConf, SparkContext
import time

# 初始化SparkContext
conf = SparkConf().setAppName("RDD_Operator_Type").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# 1. 创建RDD
num_rdd = sc.parallelize([1, 2, 3, 4, 5])

# 2. 测试Transformation算子（map）：惰性求值
print("===== 测试Transformation算子（map） =====")
start_time = time.time()
mapped_rdd = num_rdd.map(lambda x: x * 2)
end_time = time.time()
print(f"map转换执行耗时：{end_time - start_time:.6f}s（仅记录操作，未触发计算）")

# 3. 测试Action算子（count）：触发计算
print("\n===== 测试Action算子（count） =====")
start_time = time.time()
count_result = mapped_rdd.count()
end_time = time.time()
print(f"count执行耗时：{end_time - start_time:.6f}s（触发实际计算）")
print(f"mapped_rdd元素个数：{count_result}")
print(f"mapped_rdd计算结果：{mapped_rdd.collect()}")

# 4. 查看RDD血缘关系
print("\n===== RDD血缘关系 =====")
print(mapped_rdd.toDebugString().decode("utf-8"))

# 5. 验证重复计算
print("\n===== 验证重复计算 =====")
start1 = time.time()
mapped_rdd.take(3)
t1 = time.time() - start1
start2 = time.time()
mapped_rdd.take(3)
t2 = time.time() - start2
print(f"第一次take耗时：{t1:.6f}s")
print(f"第二次take耗时：{t2:.6f}s（未缓存，重复计算）")

# 关闭SparkContext
sc.stop()
print("\n✅ 实验二执行完成！")
