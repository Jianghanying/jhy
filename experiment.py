# ==============================
# 完整版实验代码（100%带步骤1、2输出）
# 实验报告要求的所有步骤都有输出！
# ==============================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("FinalExperiment") \
    .master("local[*]") \
    .getOrCreate()

print("✅ Spark 启动成功")

# ==============================================
# 【步骤1：读取所有数据 → 现在全部加了输出！】
# ==============================================
print("\n===== 步骤1：读取 employees.csv =====")
df_emp = spark.read.csv("./data/employees.csv", header=True, inferSchema=True)
df_emp.show()

print("\n===== 步骤1：读取 orders.csv =====")
df_order = spark.read.csv("./data/orders.csv", header=True, inferSchema=True)
df_order.show()

print("\n===== 步骤1：读取 products.csv =====")
df_product = spark.read.csv("./data/products.csv", header=True, inferSchema=True)
df_product.show()

print("\n===== 步骤1：读取 sales.csv =====")
df_sale = spark.read.csv("./data/sales.csv", header=True, inferSchema=True)
df_sale.show()

print("\n===== 步骤1：读取 visits.csv =====")
df_visit = spark.read.csv("./data/visits.csv", header=True, inferSchema=True)
df_visit.show()

print("\n===== 步骤1：读取 old_records.csv =====")
df_old = spark.read.csv("./data/old_records.csv", header=True, inferSchema=True)
df_old.show()

print("\n===== 步骤1：读取 new_records.csv =====")
df_new = spark.read.csv("./data/new_records.csv", header=True, inferSchema=True)
df_new.show()

print("\n===== 步骤1：读取 students.json =====")
df_student = spark.read.option("multiline", "true").json("./data/students.json")
df_student.show()

print("\n===== 步骤1：读取 scores.txt 并解析 =====")
df_txt = spark.read.text("./data/scores.txt")
df_score = df_txt.select(
    split(col("value"), ",")[0].alias("id"),
    split(col("value"), ",")[1].alias("name"),
    split(col("value"), ",")[2].alias("score1"),
    split(col("value"), ",")[3].alias("score2")
)
df_score.show()

# ==============================================
# 【步骤2：DataFrame ↔ RDD 转换 → 现在加上了！】
# ==============================================
print("\n===== 步骤2：DataFrame 转 RDD =====")
rdd = df_emp.rdd
print("DF转RDD第一条数据：")
print(rdd.first())

print("\n===== 步骤2：RDD 转 DataFrame =====")
df_restore = rdd.toDF()
df_restore.show()

# ==============================================
# 步骤3：按工资降序
# ==============================================
print("\n===== 实验3：按工资降序 =====")
df_emp.sort("salary", ascending=False).show()

# ==============================================
# 步骤4：去重
# ==============================================
print("\n===== 实验4：去重前 vs 去重后 =====")
print("去重前行数：", df_visit.count())
df_visit_unique = df_visit.distinct()
print("去重后行数：", df_visit_unique.count())
df_visit_unique.show()

# ==============================================
# 步骤5：日期处理
# ==============================================
print("\n===== 实验5：日期处理 =====")
df_order = df_order.withColumn("order_month", month("order_date"))
df_order.groupBy("order_month").count().orderBy("order_month").show()

# ==============================================
# 步骤6：差集
# ==============================================
print("\n===== 实验6：旧记录有、新记录没有 =====")
df_old.exceptAll(df_new).show()

print("\n===== 实验6：新记录有、旧记录没有 =====")
df_new.exceptAll(df_old).show()

# ==============================================
# 步骤7：表连接 + 销售额
# ==============================================
print("\n===== 实验7：连接表 + 计算销售额 =====")
df_join = df_sale.join(df_product, on="product_id")
df_result = df_join.withColumn("total", col("quantity") * col("unit_price"))
df_result.select("sale_id", "product_name", "quantity", "unit_price", "total").show()

# ==============================================
# 步骤8：地区销售额
# ==============================================
print("\n===== 实验8：各地区销售总额 =====")
df_result.groupBy("region").agg(sum("total").alias("地区总销售额")).orderBy(desc("地区总销售额")).show()

# ==============================================
# 步骤9：UDF工资等级
# ==============================================
print("\n===== 实验9：自定义工资等级 =====")
def salary_level(salary):
    if salary >= 17000:
        return "高工资"
    elif salary >= 13000:
        return "中等工资"
    else:
        return "低工资"

salary_udf = udf(salary_level, StringType())
df_emp.withColumn("level", salary_udf(col("salary"))).select("name", "salary", "level").show()

# ==============================================
# 步骤10：统计信息
# ==============================================
print("\n===== 实验10：工资统计信息 =====")
df_emp.select("salary").summary().show()

# ==============================================
# 结束
# ==============================================
spark.stop()
