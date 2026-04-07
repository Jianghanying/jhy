# 实验八：高级聚合算子combineByKey的原理与应用
from pyspark import SparkConf, SparkContext

# 初始化SparkContext
conf = SparkConf().setAppName("CombineByKey_Agg").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# 1. 读取员工数据，创建键值对RDD（部门ID, 工资）
data_path = "file:///你的本地仓库路径/data/employees.txt"
emp_rdd = sc.textFile(data_path)
clean_emp_rdd = emp_rdd.filter(lambda line: len(line.strip().split(",")) == 3 and line.strip().split(",")[2].isdigit())
kv_emp_rdd = clean_emp_rdd.map(lambda line: (line.split(",")[0], float(line.split(",")[2])))

print("===== 部门-工资键值对前5条 =====")
print(kv_emp_rdd.take(5))

# 2. 定义combineByKey三大核心函数
def create_combiner(salary):
    # 初始化：(员工数, 工资总和, 最高工资, 最低工资)
    return (1, salary, salary, salary)

def merge_value(acc, salary):
    # 分区内合并
    count, total, max_sal, min_sal = acc
    return (count + 1, total + salary, max(max_sal, salary), min(min_sal, salary))

def merge_combiners(acc1, acc2):
    # 分区间合并
    count1, total1, max1, min1 = acc1
    count2, total2, max2, min2 = acc2
    return (count1+count2, total1+total2, max(max1, max2), min(min1, min2))

# 3. 执行聚合
dept_agg_rdd = kv_emp_rdd.combineByKey(create_combiner, merge_value, merge_combiners)
# 计算平均工资，格式化结果
dept_result_rdd = dept_agg_rdd.mapValues(lambda x: (
    x[0],  # 员工数
    round(x[1], 2),  # 工资总和
    round(x[1]/x[0], 2),  # 平均工资
    round(x[2], 2),  # 最高工资
    round(x[3], 2)   # 最低工资
))

print("\n===== 各部门工资多维度统计 =====")
print("部门ID | 员工数 | 工资总和 | 平均工资 | 最高工资 | 最低工资")
print("-"*60)
for res in dept_result_rdd.collect():
    dept_id = res[0]
    count, total, avg, max_sal, min_sal = res[1]
    print(f"{dept_id:<6} | {count:<4} | {total:<8} | {avg:<8} | {max_sal:<8} | {min_sal:<8}")

# 关闭SparkContext
sc.stop()
print("\n✅ 实验八执行完成！")
