# 实验七：键值对RDD的排序与二次排序
from pyspark import SparkConf, SparkContext

# 1. 初始化SparkContext
conf = SparkConf().setAppName("KV_RDD_Sort").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# 2. 读取成绩数据，数据清洗
data_path = "你的本地data目录路径/scores.txt"
score_rdd = sc.textFile(data_path)

def clean_score(line):
    line = line.strip()
    if not line or len(line.split(",")) != 3:  # 过滤空行和字段缺失
        return False
    try:
        int(line.split(",")[2])  # 验证分数为数字
        return True
    except:
        return False

clean_score_rdd = score_rdd.filter(clean_score)

# 3. 创建键值对RDD（Key：学生姓名，Value：(科目, 分数)）
kv_score_rdd = clean_score_rdd.map(lambda line: (
    line.split(",")[0], 
    (line.split(",")[1], int(line.split(",")[2]))
))
print("===== 成绩键值对RDD前5条 =====")
print(kv_score_rdd.take(5))

# 4. sortByKey算子：按姓名升序排序
print("\n===== 按姓名升序前5条 =====")
sorted_asc = kv_score_rdd.sortByKey(ascending=True)
print(sorted_asc.take(5))

# 5. 二次排序：先按姓名升序，再按分数降序（核心：复合Key）
print("\n===== 二次排序（姓名升序+分数降序）前10条 =====")
# 构建复合Key：(姓名, -分数)，升序排序等价于姓名升序、分数降序
double_sort_rdd = clean_score_rdd.map(lambda line: (
    (line.split(",")[0], -int(line.split(",")[2])),  # 复合Key
    line.split(",")[1]  # Value：科目
))
double_sorted = double_sort_rdd.sortByKey()  # 按复合Key升序排序

# 格式化结果（还原分数为正数）
formatted_result = double_sorted.map(lambda x: (x[0][0], x[1], -x[0][1]))
print(formatted_result.take(10))

# 关闭SparkContext
sc.stop()
print("\n✅ 实验七（键值对RDD排序）执行完成！")
