# ===================== 湖南科技学院 大数据推荐系统 实验四 完整代码 =====================
# 统一导入实验所需所有包（与实验参考书一致）
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, to_date, datediff, month, year, 
    current_date, date_format, sum, avg, max, min, count, udf
)
from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField

# 初始化SparkSession（全局唯一，本地模式运行，适配网页端）
spark = SparkSession.builder \
    .appName("Experiment4-PySpark-DataFrame") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g")  # 适配网页端内存，避免报错
    .getOrCreate()
# 关闭冗余日志，只看关键输出
spark.sparkContext.setLogLevel("WARN")

print("="*60)
print("实验四：PySpark中的DataFrame - 所有环节开始运行")
print("="*60 + "\n")

# ============================================================================
# 实验一：通过多种数据源创建DataFrame（JSON/列表/RDD/文本文件）
# ============================================================================
print("【实验一】多种数据源创建DataFrame")
print("-"*50)
# 1.1 从JSON文件创建（students.json）
df_json = spark.read.option("multiline", "true").json("data/students.json")
print("1.1 从JSON读取学生数据：")
df_json.show()
print("JSON数据Schema：")
df_json.printSchema()

# 1.2 从Python可迭代对象创建（课程信息）
course_data = [("大数据技术", 4, "王教授"), ("机器学习", 3, "李教授"), ("数据挖掘", 3, "张教授")]
df_course = spark.createDataFrame(course_data, schema=["course", "credit", "teacher"])
print("\n1.2 从Python列表创建课程数据：")
df_course.show()

# 1.3 从RDD创建（城市人口数据）
city_data = [("北京", 2154), ("上海", 2487), ("广州", 1868), ("深圳", 1756)]
rdd_city = spark.sparkContext.parallelize(city_data)
df_city = rdd_city.toDF(["city", "population"])
print("\n1.3 从RDD创建城市人口数据：")
df_city.show()

# 1.4 从本地文本文件创建（scores.txt，指定Schema避免推断错误）
df_score = spark.read.text("data/scores.txt")
df_score = df_score.select(
    split(col("value"), ",").getItem(0).cast(IntegerType()).alias("student_id"),
    split(col("value"), ",").getItem(1).alias("name"),
    split(col("value"), ",").getItem(2).cast(IntegerType()).alias("regular_score"),
    split(col("value"), ",").getItem(3).cast(IntegerType()).alias("exam_score")
)
print("\n1.4 从TXT读取成绩数据：")
df_score.show()
print("成绩数据Schema：")
df_score.printSchema()

# ============================================================================
# 实验二：DataFrame与RDD的相互转换
# ============================================================================
print("\n【实验二】DataFrame与RDD的相互转换")
print("-"*50)
# 2.1 DataFrame转RDD，访问Row对象
rdd_json = df_json.rdd
first_row = rdd_json.first()
print("2.1 DataFrame转RDD - 第一条数据类型：", type(first_row))
print("通过字段名访问姓名：", first_row.name)
print("通过索引访问姓名：", first_row[1])
print("前3条RDD数据：")
for row in rdd_json.take(3):
    print(row)

# 2.2 RDD转回DataFrame（指定原Schema，保证结构一致）
df_restored = spark.createDataFrame(rdd_json, schema=df_json.schema)
print("\n2.2 RDD转回DataFrame（Schema与原数据一致）：")
df_restored.show()

# ============================================================================
# 实验三：使用orderBy实现多字段排序（单字段/多字段组合）
# ============================================================================
print("\n【实验三】orderBy多字段排序")
print("-"*50)
df_emp = spark.read.csv("data/employees.csv", header=True, inferSchema=True)
# 3.1 单字段排序：薪资升序/降序
print("3.1 薪资升序：")
df_emp.orderBy("salary").select("name", "department", "salary").show()
print("薪资降序：")
df_emp.orderBy(col("salary").desc()).select("name", "department", "salary").show()

# 3.2 多字段组合排序：部门升序+薪资降序
print("\n3.2 部门升序+薪资降序：")
df_emp.orderBy(col("department").asc(), col("salary").desc()).select("name", "department", "salary").show()

# ============================================================================
# 实验四：处理日期类型数据（解析/格式化/计算/分组）
# ============================================================================
print("\n【实验四】日期类型数据处理")
print("-"*50)
df_order = spark.read.csv("data/orders.csv", header=True, inferSchema=True)
# 4.1 解析日期：将字符串转为Date类型
df_order = df_order.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
print("4.1 日期类型解析后Schema：")
df_order.printSchema()

# 4.2 日期格式化：转为「yyyy年MM月dd日」格式
df_order = df_order.withColumn("order_date_fmt", date_format(col("order_date"), "yyyy年MM月dd日"))
print("\n4.2 日期格式化结果：")
df_order.select("order_id", "order_date", "order_date_fmt").show()

# 4.3 日期计算：距今天数+提取月份，按月份统计订单
df_order = df_order.withColumn("days_since", datediff(current_date(), col("order_date"))) \
                   .withColumn("order_month", month(col("order_date")))
print("\n4.3 日期计算+按月统计订单数量：")
df_order.groupBy("order_month").count().alias("order_count").show()

# ============================================================================
# 实验五：使用distinct消除重复行（完全去重+指定列去重）
# ============================================================================
print("\n【实验五】distinct数据去重")
print("-"*50)
df_visit = spark.read.csv("data/visits.csv", header=True, inferSchema=True)
print(f"5.1 原始访问日志行数：{df_visit.count()}")
# 5.2 distinct完全去重
df_visit_dist = df_visit.distinct()
print(f"5.2 完全去重后行数：{df_visit_dist.count()}")
print("去重后访问日志：")
df_visit_dist.select("user_id", "page_url", "duration").show()

# ============================================================================
# 实验六：使用drop删除单个/多个列
# ============================================================================
print("\n【实验六】drop删除列")
print("-"*50)
df_emp_drop = spark.read.csv("data/employees.csv", header=True, inferSchema=True)
print(f"6.1 原始列名：{df_emp_drop.columns}")
# 6.2 删除单个列
df_emp_drop1 = df_emp_drop.drop("join_date")
# 6.3 删除多个列
df_emp_drop2 = df_emp_drop.drop("emp_id", "join_date")
print(f"6.2 删除join_date后列名：{df_emp_drop1.columns}")
print(f"6.3 删除emp_id+join_date后列名：{df_emp_drop2.columns}")
print("精简后员工数据：")
df_emp_drop2.show()

# ============================================================================
# 实验七：使用exceptAll进行差集运算（新旧库存对比）
# ============================================================================
print("\n【实验七】exceptAll集合差集运算")
print("-"*50)
df_old = spark.read.csv("data/old_records.csv", header=True, inferSchema=True)
df_new = spark.read.csv("data/new_records.csv", header=True, inferSchema=True)
print("旧库存记录：")
df_old.show()
print("新库存记录：")
df_new.show()
# 旧-新：消失的库存
df_old_minus_new = df_old.exceptAll(df_new)
print("\n旧记录 - 新记录（消失的库存）：")
df_old_minus_new.show()
# 新-旧：新增的库存
df_new_minus_old = df_new.exceptAll(df_old)
print("新记录 - 旧记录（新增/重复的库存）：")
df_new_minus_old.show()

# ============================================================================
# 实验八：多表关联+分组聚合（内连接+单/多维度聚合）
# ============================================================================
print("\n【实验八】多表内连接+分组聚合")
print("-"*50)
df_sale = spark.read.csv("data/sales.csv", header=True, inferSchema=True)
df_prod = spark.read.csv("data/products.csv", header=True, inferSchema=True)
# 8.1 内连接：销售表+产品表（按product_id）
df_sale_prod = df_sale.join(df_prod, on="product_id", how="inner") \
                     .select("sale_id", "product_name", "category", "quantity", "unit_price", "region")
print("8.1 销售表-产品表内连接结果：")
df_sale_prod.show()

# 8.2 新增销售总额+单维度聚合（按区域）
df_sale_prod = df_sale_prod.withColumn("total_sales", col("quantity") * col("unit_price"))
df_region_agg = df_sale_prod.groupBy("region") \
                           .agg(
                               sum("total_sales").alias("区域总额"),
                               count("sale_id").alias("订单数"),
                               avg("total_sales").alias("平均订单金额")
                           )
print("\n8.2 按区域聚合统计：")
df_region_agg.show()

# 8.3 多维度聚合（区域+品类）
df_region_cat_agg = df_sale_prod.groupBy("region", "category") \
                               .agg(sum("total_sales").alias("品类总额")) \
                               .orderBy(col("品类总额").desc())
print("\n8.3 按区域+品类聚合统计（降序）：")
df_region_cat_agg.show()

# ============================================================================
# 实验九：UDF用户自定义函数（定义+注册+筛选使用）
# ============================================================================
print("\n【实验九】UDF自定义函数（薪资等级判断）")
print("-"*50)
df_emp_udf = spark.read.csv("data/employees.csv", header=True, inferSchema=True)
# 9.1 定义Python函数+注册UDF
def salary_level(salary):
    if salary >= 17000:
        return "高级"
    elif salary >= 13000:
        return "中级"
    else:
        return "初级"
salary_udf = udf(salary_level, StringType())
# 新增薪资等级列
df_emp_udf = df_emp_udf.withColumn("薪资等级", salary_udf(col("salary")))
print("9.1 新增薪资等级列结果：")
df_emp_udf.select("name", "department", "salary", "薪资等级").show()

# 9.2 UDF筛选：只看高级员工
df_high = df_emp_udf.where(salary_udf(col("salary")) == "高级")
print("\n9.2 UDF筛选-高级员工：")
df_high.select("name", "department", "salary").show()

# ============================================================================
# 实验十：列操作+描述性统计（withColumn/重命名/describe/summary）
# ============================================================================
print("\n【实验十】列操作+描述性统计")
print("-"*50)
df_emp_col = spark.read.csv("data/employees.csv", header=True, inferSchema=True)
# 10.1 withColumn新增列：年薪+税后月薪
df_emp_col = df_emp_col.withColumn("年薪", col("salary")*12) \
                       .withColumn("税后月薪", col("salary")*0.8)
print("10.1 新增年薪/税后月薪列：")
df_emp_col.select("name", "salary", "年薪", "税后月薪").show()

# 10.2 withColumnRenamed重命名列
df_emp_rename = df_emp_col.withColumnRenamed("name", "员工姓名").withColumnRenamed("department", "部门")
print(f"\n10.2 列重命名后列名：{df_emp_rename.columns}")

# 10.3 selectExpr：SQL表达式计算
df_emp_sql = df_emp_col.selectExpr("name", "salary * 12 + 5000 AS 年薪（含年终奖）")
print("\n10.3 selectExpr SQL表达式计算：")
df_emp_sql.show()

# 10.4 describe基础统计：薪资+年龄
print("\n10.4 describe基础描述性统计（薪资+年龄）：")
df_emp_col.describe("salary", "age").show()

# 10.5 summary详细统计：薪资（含四分位数）
print("\n10.5 summary详细统计（薪资，含四分位数）：")
df_emp_col.select("salary").summary("count", "mean", "min", "25%", "50%", "75%", "max").show()

# ============================================================================
# 实验收尾：关闭SparkSession，释放资源
# ============================================================================
spark.stop()
print("\n" + "="*60)
print("实验四：PySpark中的DataFrame - 所有环节运行完成！")
print("="*60)
