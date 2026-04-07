# ===================== PySpark 实验代码 =====================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_date, sum, avg, udf
from pyspark.sql.types import StringType

# 创建 Spark
spark = SparkSession.builder \
    .appName("Experiment") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==============================================
# 1. 读取员工数据
print("=== 1. 员工数据 ===")
df_emp = spark.read.csv("data/employees.csv", header=True, inferSchema=True)
df_emp.show()
df_emp.printSchema()

# ==============================================
# 2. 读取订单数据 + 日期处理
print("=== 2. 订单数据 ===")
df_order = spark.read.csv("data/orders.csv", header=True, inferSchema=True)
df_order = df_order.withColumn("order_date", to_date(col("order_date")))
df_order.show()

# ==============================================
# 3. 销售 + 产品关联
print("=== 3. 销售与产品表连接 ===")
df_sale = spark.read.csv("data/sales.csv", header=True, inferSchema=True)
df_prod = spark.read.csv("data/products.csv", header=True, inferSchema=True)
df_join = df_sale.join(df_prod, on="product_id")
df_join.show()

# ==============================================
# 4. 分组统计
print("=== 4. 区域销售统计 ===")
df_join = df_join.withColumn("total", col("quantity") * col("unit_price"))
df_agg = df_join.groupBy("region").agg(sum("total").alias("sales_total"))
df_agg.show()

# ==============================================
# 5. UDF 薪资等级
print("=== 5. 自定义函数 UDF ===")
def level(salary):
    if salary >= 17000:
        return "高级"
    elif salary >= 13000:
        return "中级"
    else:
        return "初级"

level_udf = udf(level, StringType())
df_emp = df_emp.withColumn("level", level_udf(col("salary")))
df_emp.select("name","salary","level").show()

# ==============================================
# 6. 去重
print("=== 6. 访问记录去重 ===")
df_visit = spark.read.csv("data/visits.csv", header=True, inferSchema=True)
df_visit.distinct().show()

# ==============================================
# 7. 差集 exceptAll
print("=== 7. 新旧记录差集 ===")
df_old = spark.read.csv("data/old_records.csv", header=True, inferSchema=True)
df_new = spark.read.csv("data/new_records.csv", header=True, inferSchema=True)
df_old.exceptAll(df_new).show()

# ==============================================
# 8. JSON、TXT 读取
print("=== 8. JSON 学生数据 ===")
df_stu = spark.read.option("multiline", "true").json("data/students.json")
df_stu.show()

print("=== 9. TXT 成绩 ===")
df_score = spark.read.text("data/scores.txt")
df_score = df_score.select(
    split(col("value"), ",").getItem(0).alias("id"),
    split(col("value"), ",").getItem(1).alias("name"),
    split(col("value"), ",").getItem(2).alias("regular"),
    split(col("value"), ",").getItem(3).alias("exam")
)
df_score.show()

print("✅ 实验全部完成！")
spark.stop()
