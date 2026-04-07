# 实验一：RDD的创建与基础数据预处理
from pyspark import SparkConf, SparkContext

# 1. 初始化SparkContext（本地模式，使用所有CPU核心）
conf = SparkConf().setAppName("RDD_Create_Clean").setMaster("local[*]")
sc = SparkContext(conf=conf)
# 关闭冗余日志，仅显示错误信息（避免干扰输出）
sc.setLogLevel("ERROR")

# 2. 从本地文件创建RDD（需替换为你的本地仓库data目录路径）
# 示例：Windows路径 r"C:\Users\你的用户名\Downloads\spark-rdd-lab\data\employees.txt"
# 示例：Ubuntu路径 "/home/你的用户名/spark-rdd-lab/data/employees.txt"
data_path = "你的本地data目录路径/employees.txt"
emp_rdd = sc.textFile(data_path)

# 3. 预览原始数据，统计原始数据量
print("===== 原始数据前5条 =====")
print(emp_rdd.take(5))  # 安全预览前5条，避免大数据集溢出
original_count = emp_rdd.count()
print(f"原始数据总条数：{original_count}")

# 4. 定义清洗规则，过滤脏数据（空行、字段缺失、工资非数字）
def clean_data(line):
    line = line.strip()  # 去除首尾空格
    if not line:  # 过滤空行
        return False
    fields = line.split(",")  # 按逗号分割字段
    if len(fields) != 3:  # 过滤字段数不为3的行
        return False
    try:
        float(fields[2])  # 验证工资为数字
        return True
    except ValueError:
        return False

# 应用filter算子清洗数据
cleaned_emp_rdd = emp_rdd.filter(clean_data)

# 5. 预览清洗后数据，验证清洗效果
print("\n===== 清洗后数据前5条 =====")
print(cleaned_emp_rdd.take(5))
cleaned_count = cleaned_emp_rdd.count()
print(f"清洗后数据总条数：{cleaned_count}")
print(f"过滤脏数据条数：{original_count - cleaned_count}")

# 6. 格式化数据为元组（部门ID, 姓名, 工资），便于后续计算
emp_tuple_rdd = cleaned_emp_rdd.map(lambda x: (x.split(",")[0], x.split(",")[1], int(x.split(",")[2])))
print("\n===== 格式化后数据前5条 =====")
print(emp_tuple_rdd.take(5))

# 7. 关闭SparkContext，释放资源
sc.stop()
print("\n✅ 实验一（RDD创建与数据预处理）执行完成！")
