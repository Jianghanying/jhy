# 实验七：键值对RDD的排序与二次排序
from pyspark import SparkConf, SparkContext

# 初始化SparkContext
conf = SparkConf().setAppName("KV_RDD_Sort").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# 1. 读取成绩数据
data_path = "file:///你的本地仓库路径/data/scores.txt"
score_rdd = sc.textFile(data_path)

# 2. 数据清洗
def clean_score(line):
    line = line.strip()
    if not line or len(line.split(",")) != 3:
        return False
    try:
        int(line.split(",")[2])
        return True
    except:
        return False

clean_score_rdd = score_rdd.filter(clean_score)

# 3. 创建键值对RDD（姓名, (科目, 分数)）
kv_score_rdd = clean_score_rdd.map(lambda line: (line.split(",")[0], (line.split(",")[1], int(line.split(",")[2]))))
print("===== 成绩键值对RDD前5条 =====")
print(kv_score_rdd.take(5))

# 4. sortByKey按姓名排序
sorted_asc = kv_score_rdd.sortByKey(ascending=True)
print("\n===== 按姓名升序前5条 =====")
print(sorted_asc.take(5))

# 5. 二次排序（姓名升序+分数降序）
double_sort_rdd = clean_score_rdd.map(lambda line: (
    (line.split(",")[0], -int(line.split(",")[2])),  # 复合Key
    line.split(",")[1]
))
double_sorted = double_sort_rdd.sortByKey()
formatted_result = double_sorted.map(lambda x: (x[0][0], x[1], -x[0][1]))

print("\n===== 二次排序（姓名升序+分数降序）前10条 =====")
print(formatted_result.take(10))

# 关闭SparkContext
sc.stop()
print("\n✅ 实验七执行完成！")
