from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    count,
    trim,
    upper,
    round
)
import os


# ============================================================
# 1. TẠO SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Lab03_Sales")
    .master("local[*]")
    .getOrCreate()
)

print("=" * 70)
print("SPARK SESSION CREATED SUCCESSFULLY!")
print("=" * 70)


# ============================================================
# 2. XÁC ĐỊNH ĐƯỜNG DẪN FILE CSV
# ============================================================

file_name = "sample_sales_bigdata_lab03.csv"

# Trường hợp chạy từ thư mục bigdata_lab
path_1 = os.path.join("lab03", file_name)

# Trường hợp chạy trực tiếp từ thư mục lab03
path_2 = file_name

if os.path.exists(path_1):
    csv_path = path_1
elif os.path.exists(path_2):
    csv_path = path_2
else:
    print("KHONG TIM THAY FILE CSV!")
    print("Hay kiem tra file:", file_name)
    spark.stop()
    raise FileNotFoundError(file_name)

print("\nCSV FILE:")
print(csv_path)


# ============================================================
# 3. ĐỌC FILE CSV
# ============================================================

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("encoding", "UTF-8")
    .csv(csv_path)
)


# ============================================================
# 4. KIỂM TRA SCHEMA
# ============================================================

print("\n" + "=" * 70)
print("SCHEMA")
print("=" * 70)

df.printSchema()


# ============================================================
# 5. ĐẾM SỐ DÒNG
# ============================================================

print("\n" + "=" * 70)
print("NUMBER OF ROWS")
print("=" * 70)

total_rows = df.count()

print("Number of rows:", total_rows)


# ============================================================
# 6. HIỂN THỊ 5 DÒNG ĐẦU
# ============================================================

print("\n" + "=" * 70)
print("FIRST 5 ROWS")
print("=" * 70)

df.show(5, truncate=False)


# ============================================================
# 7. KIỂM TRA TÊN CÁC CỘT
# ============================================================

print("\n" + "=" * 70)
print("COLUMNS")
print("=" * 70)

print(df.columns)


# ============================================================
# 8. KIỂM TRA STATUS
# ============================================================

print("\n" + "=" * 70)
print("STATUS")
print("=" * 70)

df.groupBy("status") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(truncate=False)


# ============================================================
# 9. KIỂM TRA GIÁ TRỊ NULL
# ============================================================

print("\n" + "=" * 70)
print("NULL VALUES")
print("=" * 70)

for column_name in df.columns:
    null_count = df.filter(
        col(column_name).isNull()
    ).count()

    print(f"{column_name}: {null_count}")


# ============================================================
# 10. CHUẨN HÓA STATUS
# ============================================================

df_clean = df.withColumn(
    "status",
    upper(trim(col("status")))
)


# ============================================================
# 11. LÀM SẠCH DỮ LIỆU
# ============================================================
#
# Chỉ lấy đơn hàng COMPLETED
# quantity > 0
# unit_price > 0
# city không NULL
# category không NULL
# product không NULL
#
# discount NULL sẽ được xem là 0
# ============================================================

df_clean = df_clean.fillna(
    {
        "discount": 0
    }
)

df_clean = df_clean.filter(
    (col("status") == "COMPLETED") &
    (col("quantity") > 0) &
    (col("unit_price") > 0) &
    col("city").isNotNull() &
    col("category").isNotNull() &
    col("product").isNotNull()
)


# ============================================================
# 12. KIỂM TRA SAU KHI LÀM SẠCH
# ============================================================

print("\n" + "=" * 70)
print("DATA AFTER CLEANING")
print("=" * 70)

clean_rows = df_clean.count()

print("Rows before cleaning:", total_rows)
print("Rows after cleaning :", clean_rows)
print("Rows removed        :", total_rows - clean_rows)

df_clean.show(5, truncate=False)


# ============================================================
# 13. TẠO CỘT REVENUE
# ============================================================
#
# Công thức:
#
# revenue =
# quantity * unit_price * (1 - discount)
#
# ============================================================

df_clean = df_clean.withColumn(
    "revenue",
    round(
        col("quantity") *
        col("unit_price") *
        (1 - col("discount")),
        2
    )
)


# ============================================================
# 14. KIỂM TRA REVENUE
# ============================================================

print("\n" + "=" * 70)
print("REVENUE")
print("=" * 70)

df_clean.select(
    "order_id",
    "order_date",
    "city",
    "category",
    "product",
    "quantity",
    "unit_price",
    "discount",
    "revenue"
).show(10, truncate=False)


# ============================================================
# 15. TỔNG DOANH THU
# ============================================================

print("\n" + "=" * 70)
print("TOTAL REVENUE")
print("=" * 70)

total_revenue = df_clean.select(
    round(
        sum("revenue"),
        2
    ).alias("total_revenue")
)

total_revenue.show(truncate=False)


# ============================================================
# 16. DOANH THU THEO THÀNH PHỐ
# ============================================================

print("\n" + "=" * 70)
print("REVENUE BY CITY")
print("=" * 70)

revenue_by_city = (
    df_clean
    .groupBy("city")
    .agg(
        round(
            sum("revenue"),
            2
        ).alias("total_revenue"),

        count("*").alias("number_of_orders")
    )
    .orderBy(
        col("total_revenue").desc()
    )
)

revenue_by_city.show(
    truncate=False
)


# ============================================================
# 17. DOANH THU THEO DANH MỤC
# ============================================================

print("\n" + "=" * 70)
print("REVENUE BY CATEGORY")
print("=" * 70)

revenue_by_category = (
    df_clean
    .groupBy("category")
    .agg(
        round(
            sum("revenue"),
            2
        ).alias("total_revenue"),

        count("*").alias("number_of_orders")
    )
    .orderBy(
        col("total_revenue").desc()
    )
)

revenue_by_category.show(
    truncate=False
)


# ============================================================
# 18. DOANH THU THEO THÀNH PHỐ + DANH MỤC
# ============================================================

print("\n" + "=" * 70)
print("REVENUE BY CITY AND CATEGORY")
print("=" * 70)

revenue_city_category = (
    df_clean
    .groupBy(
        "city",
        "category"
    )
    .agg(
        round(
            sum("revenue"),
            2
        ).alias("total_revenue"),

        count("*").alias("number_of_orders")
    )
    .orderBy(
        col("total_revenue").desc()
    )
)

revenue_city_category.show(
    30,
    truncate=False
)


# ============================================================
# 19. DOANH THU THEO SẢN PHẨM
# ============================================================

print("\n" + "=" * 70)
print("REVENUE BY PRODUCT")
print("=" * 70)

revenue_by_product = (
    df_clean
    .groupBy(
        "product"
    )
    .agg(
        round(
            sum("revenue"),
            2
        ).alias("total_revenue"),

        count("*").alias("number_of_orders")
    )
    .orderBy(
        col("total_revenue").desc()
    )
)

revenue_by_product.show(
    20,
    truncate=False
)


# ============================================================
# 20. SPARK SQL
# ============================================================

print("\n" + "=" * 70)
print("SPARK SQL")
print("=" * 70)


# Tạo temporary view
df_clean.createOrReplaceTempView("sales")


# ============================================================
# 21. SQL: DOANH THU THEO THÀNH PHỐ + DANH MỤC
# ============================================================

sql_result = spark.sql("""
    SELECT
        city,
        category,
        ROUND(SUM(revenue), 2) AS total_revenue,
        COUNT(*) AS number_of_orders
    FROM sales
    GROUP BY
        city,
        category
    ORDER BY
        total_revenue DESC
""")


print("\n===== SQL RESULT =====")

sql_result.show(
    30,
    truncate=False
)


# ============================================================
# 22. SQL: TỔNG DOANH THU THEO THÀNH PHỐ
# ============================================================

sql_city = spark.sql("""
    SELECT
        city,
        ROUND(SUM(revenue), 2) AS total_revenue,
        COUNT(*) AS number_of_orders
    FROM sales
    GROUP BY city
    ORDER BY total_revenue DESC
""")


print("\n===== SQL - REVENUE BY CITY =====")

sql_city.show(
    truncate=False
)


# ============================================================
# 23. SQL: TỔNG DOANH THU THEO DANH MỤC
# ============================================================

sql_category = spark.sql("""
    SELECT
        category,
        ROUND(SUM(revenue), 2) AS total_revenue,
        COUNT(*) AS number_of_orders
    FROM sales
    GROUP BY category
    ORDER BY total_revenue DESC
""")


print("\n===== SQL - REVENUE BY CATEGORY =====")

sql_category.show(
    truncate=False
)


# ============================================================
# 24. TẠO THƯ MỤC OUTPUT
# ============================================================

output_dir = "output"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)


# ============================================================
# 25. XUẤT KẾT QUẢ - DOANH THU THEO THÀNH PHỐ
# ============================================================

print("\n" + "=" * 70)
print("EXPORT REVENUE BY CITY")
print("=" * 70)

revenue_by_city.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        os.path.join(
            output_dir,
            "revenue_by_city"
        )
    )


# ============================================================
# 26. XUẤT KẾT QUẢ - DOANH THU THEO DANH MỤC
# ============================================================

print("\n" + "=" * 70)
print("EXPORT REVENUE BY CATEGORY")
print("=" * 70)

revenue_by_category.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        os.path.join(
            output_dir,
            "revenue_by_category"
        )
    )


# ============================================================
# 27. XUẤT KẾT QUẢ - THÀNH PHỐ + DANH MỤC
# ============================================================

print("\n" + "=" * 70)
print("EXPORT REVENUE BY CITY AND CATEGORY")
print("=" * 70)

revenue_city_category.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        os.path.join(
            output_dir,
            "revenue_city_category"
        )
    )


# ============================================================
# 28. XUẤT KẾT QUẢ SQL
# ============================================================

print("\n" + "=" * 70)
print("EXPORT SQL RESULT")
print("=" * 70)

sql_result.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        os.path.join(
            output_dir,
            "sql_result"
        )
    )


# ============================================================
# 29. THÔNG BÁO HOÀN THÀNH
# ============================================================

print("\n" + "=" * 70)
print("ALL PROCESSING COMPLETED SUCCESSFULLY!")
print("=" * 70)

print("\nOutput folders:")

print(
    os.path.join(
        output_dir,
        "revenue_by_city"
    )
)

print(
    os.path.join(
        output_dir,
        "revenue_by_category"
    )
)

print(
    os.path.join(
        output_dir,
        "revenue_city_category"
    )
)

print(
    os.path.join(
        output_dir,
        "sql_result"
    )
)


# ============================================================
# 30. DỪNG SPARK
# ============================================================
#
# CỰC KỲ QUAN TRỌNG:
# spark.stop() phải nằm CUỐI CÙNG.
# ============================================================

spark.stop()

print("\nSparkSession stopped.")
print("Program finished.")