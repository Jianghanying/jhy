# 实验一：RDD的创建与基础数据预处理
from pyspark import SparkConf, SparkContext

# 初始化SparkContext（本地模式，不连HDFS，避免报错）
conf = SparkConf().setAppName("RDD_Create_Clean").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# 1. 读取本地数据（GitHub下载后替换为你的本地路径）
data_path = "file:///你的本地仓库路径/data/employees.txt"
emp_rdd = sc.textFile(data_path)

# 2. 查看原始数据
print("===== 原始数据前5条 =====")
print(emp_rdd.take(5))
original_count = emp_rdd.count()
print(f"原始数据总条数：{original_count}")

# 3. 数据清洗（过滤空行、字段缺失、工资非数字）
def clean_data(line):
    line = line.strip()
    if not line:
        return False
    parts = line.split(",")
    if len(parts) != 3:
        return False
    return parts[2].isdigit()

cleaned_emp_rdd = emp_rdd.filter(clean_data)

# 4. 查看清洗后数据
print("\n===== 清洗后数据前5条 =====")
print(cleaned_emp_rdd.take(5))
cleaned_count = cleaned_emp_rdd.count()
print(f"清洗后数据总条数：{cleaned_count}")
print(f"过滤脏数据条数：{original_count - cleaned_count}")

# 5. 格式化数据（部门ID, 姓名, 工资）
emp_tuple_rdd = cleaned_emp_rdd.map(lambda x: (x.split(",")[0], x.split(",")[1], int(x.split(",")[2])))
print("\n===== 格式化后数据前5条 =====")
print(emp_tuple_rdd.take(5))

# 关闭SparkContext
sc.stop()
print("\n✅ 实验一执行完成！")
