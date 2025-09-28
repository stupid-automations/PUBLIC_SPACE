1700
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> TOPIC
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

orderBy()
groupBy()
filter()
withColumn()
withColumns()

Setup and Basics
SparkSession, DataFrame creation, and basic operations.

DataFrame Operations
Selecting, filtering, grouping, joining, sorting, and aggregating.

SQL Expressions
Using expr() and selectExpr() for SQL-like transformations.

Column Transformations
Using withColumn() and withColumns() for adding/modifying columns.

Joins and Combining Data
Types of joins (inner, left, right, outer, etc.).

Aggregations and Grouping
GroupBy, aggregations (sum, count, avg, etc.), and window functions.

Handling Nulls and Missing Data
Managing null values with COALESCE, na.drop(), etc.

Date and Time Operations
Date manipulations using SQL functions.

Complex Data Types
Arrays, structs, and maps.

User-Defined Functions (UDFs)
Custom Python functions for DataFrame operations.

Performance Optimization
Caching, partitioning, and broadcast joins.

Spark SQL
Running SQL queries directly on DataFrames.
Input/Output Operations
Reading/writing data (CSV, JSON, Parquet, etc.).

Streaming
Structured Streaming for real-time data processing.

Machine Learning with MLlib
Basic ML pipelines and transformations.



+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> WHEN WITH SELECT, ALIAS, ISNUL
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql.functions import col, when

df = df.select(
    col("id").alias("user_id"),
    col("name").alias("full_name"),
    col("status").alias("account_status"),
    when((col("record").isNull()) | (col("record") == ""), "ER")
        .otherwise(col("record"))
        .alias("record_cleaned")
)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> DATABASE, ROLLBACK, COMMIT, CONNECTION
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

jdbc_url = "jdbc:postgresql://host:port/db"
conn_props = {
    "user": "your_user",
    "password": "your_pass",
    "driver": "org.postgresql.Driver"
}

# Get raw Java connection
conn = spark._sc._jvm.java.sql.DriverManager.getConnection(jdbc_url, conn_props["user"], conn_props["password"])
conn.setAutoCommit(False)

try:
    stmt = conn.createStatement()
    
    # Use SQL insert manually — or call stored procedure
    stmt.executeUpdate("INSERT INTO table1 (col1, col2) VALUES ('a', 'b')")
    stmt.executeUpdate("INSERT INTO table2 (col1, col2) VALUES ('x', 'y')")
    
    conn.commit()
    print("Transaction successful")
except Exception as e:
    conn.rollback()
    print(f"Transaction rolled back due to error: {e}")
finally:
    conn.close()


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> IF, WHEN, OTHERWISE
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

if condition1:
    if condition1a:
        result = value1a
    elif condition1b:
        result = value1b
    else:
        result = value1_default
else:
    result = value_outside

withColumn(
  "status",
  when(col("score") > 90, "A")
  .when(col("score") > 80, "B")
  .when(col("score") > 70, "C")
  .otherwise("F")
)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> EMPTY, LINE, BLANK, LAMBDA 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql.functions import col

df.select(
    col("your_column").cast("date").alias("your_date_column")
)

---

non_empty_condition = ~(
    reduce(
        lambda a, b: a & b,
        [(col(c).isNull() | (col(c) == "")) for c in df.columns]
    )
)

df_cleaned = df.filter(non_empty_condition)

###

df_cleaned = df9.filter(
    ~(
        (col("product_type").isNull()      | ( trim(col("product_type")) == ""))   &
        (col("reserve_ag_dlr").isNull()    | ( trim(col("reserve_ag_dlr")) == "")) &
        (col("reserve_date").isNull()      | ( trim(col("reserve_date")) == "")) 
    )
)

---

df9 = df_IN_raw.select(
    when((col("product_type").isNull()) | (trim(col("product_type")) == ""), lit(" ")).otherwise(col("product_type")).alias("product_type"),
    when((col("defect_code").isNull())  | (trim(col("defect_code")) == ""), lit(" ")).otherwise(col("defect_code")).alias("defect_code"),
)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> RENAME, SELECT, DISTINCT, CREATE, 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql.functions import col, when, lit

# Sample df1
df1 = spark.createDataFrame([
    (1, "Rahul"),
    (2, "Amit"),
    (3, "Priya"),
    (4, "Sneha")
], ["ID", "Name"])

# Sample df2 with duplicate IDs
df2 = spark.createDataFrame([
    (1, 85),
    (3, 90),
    (3, 95),
    (5, 70)
], ["ID", "Marks"])

# Get distinct IDs from df2
df2_unique_ids = df2.select("ID").distinct()

# Left join df1 with distinct df2 IDs
df_joined = df1.join(df2_unique_ids.withColumnRenamed("ID", "df2_ID"), df1.ID == col("df2_ID"), "left")

# Create 'Exists_in_df2' column
df_result = df_joined.withColumn(
    "Exists_in_df2",
    when(col("df2_ID").isNotNull(), lit("Yes")).otherwise(lit("No"))
)

# Select final columns
df_result.select("ID", "Name", "Exists_in_df2").show()

[Note]
df1.ID works fine — it’s like a shortcut.

col("df2_ID") is used when the column is not directly referenced from a DataFrame object, or it's renamed.

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> FILTER
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql.functions import col

# Perform inner join on id
joined_df = input_df.alias("in").join(
    output_df.alias("out"),
    on="id",
    how="left"
)

# Filter only rows where at least one column doesn't match
filtered_df = joined_df.filter(
    (col("in.name")    != col("out.name"))  |
    (col("in.age")     != col("out.age"))   |
    (col("in.salary")  != col("out.salary"))|
    (col("in.add")     != col("out.add"))   |
    col("out.id").isNull()                 # Also keep rows where ID was not found in output_df
).select("in.*")  # Keep only columns from input_df

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> EXPR, EXPR()
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Key Notes
Performance: expr() is optimized as it’s executed within the Catalyst optimizer, similar to native PySpark functions.
SQL Functions: You can use any SQL function supported by Spark SQL (e.g., UPPER, LOWER, DATEDIFF, SUM, etc.).
Limitations: Complex logic may be better handled with native PySpark functions or UDFs for readability and maintainability.
Debugging: Ensure the SQL expression syntax is correct, as errors in expr() can sometimes be cryptic.

When to Use expr()?
When you need SQL-like syntax for quick prototyping.
For dynamic expressions based on runtime conditions.

---

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Initialize Spark session
spark = SparkSession.builder.appName("ExprExamples").getOrCreate()

# Sample data
data = [
    ("Alice", 25, 50000, "2023-01-15", "F"),
    ("Bob", 30, 60000, "2022-06-20", "M"),
    ("Cathy", 28, 75000, "2021-09-10", "F"),
    ("David", None, 45000, "2020-03-05", "M")
]
columns = ["name", "age", "salary", "join_date", "gender"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
df.show()
+-----+----+------+----------+------+
| name| age|salary| join_date|gender|
+-----+----+------+----------+------+
|Alice|  25| 50000|2023-01-15|     F|
|  Bob|  30| 60000|2022-06-20|     M|
|Cathy|  28| 75000|2021-09-10|     F|
|David|null| 45000|2020-03-05|     M|
+-----+----+------+----------+------+

1. Arithmetic Operations
# Increase salary by 10% and create a new column
df_arithmetic = df.withColumn("salary_increased", expr("salary * 1.10"))
df_arithmetic.show()
+-----+----+------+----------+------+---------------+
| name| age|salary| join_date|gender|salary_increased|
+-----+----+------+----------+------+---------------+
|Alice|  25| 50000|2023-01-15|     F|        55000.0|
|  Bob|  30| 60000|2022-06-20|     M|        66000.0|
|Cathy|  28| 75000|2021-09-10|     F|        82500.0|
|David|null| 45000|2020-03-05|     M|        49500.0|
+-----+----+------+----------+------+---------------+

2. String Manipulation
# Concatenate name and gender with a separator
df_string = df.withColumn("name_gender", expr("CONCAT(name, ' - ', gender)"))
df_string.show()
+-----+----+------+----------+------+-----------+
| name| age|salary| join_date|gender|name_gender|
+-----+----+------+----------+------+-----------+
|Alice|  25| 50000|2023-01-15|     F|  Alice - F|
|  Bob|  30| 60000|2022-06-20|     M|    Bob - M|
|Cathy|  28| 75000|2021-09-10|     F|  Cathy - F|
|David|null| 45000|2020-03-05|     M|  David - M|
+-----+----+------+----------+------+-----------+

3. Conditional Logic (CASE WHEN)
# Categorize salary into Low, Medium, High
df_conditional = df.withColumn(
    "salary_category",
    expr("""
        CASE 
            WHEN salary < 50000 THEN 'Low'
            WHEN salary BETWEEN 50000 AND 70000 THEN 'Medium'
            ELSE 'High'
        END
    """)
)
df_conditional.show()
+-----+----+------+----------+------+---------------+
| name| age|salary| join_date|gender|salary_category|
+-----+----+------+----------+------+---------------+
|Alice|  25| 50000|2023-01-15|     F|         Medium|
|  Bob|  30| 60000|2022-06-20|     M|         Medium|
|Cathy|  28| 75000|2021-09-10|     F|           High|
|David|null| 45000|2020-03-05|     M|            Low|
+-----+----+------+----------+------+---------------+

4. Handling Null Values
# Replace null age with average age (approx 27.67)
df_null_handling = df.withColumn("age_filled", expr("COALESCE(age, 27)"))
df_null_handling.show()
+-----+----+------+----------+------+----------+
| name| age|salary| join_date|gender|age_filled|
+-----+----+------+----------+------+----------+
|Alice|  25| 50000|2023-01-15|     F|        25|
|  Bob|  30| 60000|2022-06-20|     M|        30|
|Cathy|  28| 75000|2021-09-10|     F|        28|
|David|null| 45000|2020-03-05|     M|        27|
+-----+----+------+----------+------+----------+

5. Date and Time Operations
# Add 30 days to join_date
df_date = df.withColumn("join_date_plus_30", expr("DATE_ADD(join_date, 30)"))
df_date.show()
+-----+----+------+----------+------+-----------------+
| name| age|salary| join_date|gender|join_date_plus_30|
+-----+----+------+----------+------+-----------------+
|Alice|  25| 50000|2023-01-15|     F|       2023-02-14|
|  Bob|  30| 60000|2022-06-20|     M|       2022-07-20|
|Cathy|  28| 75000|2021-09-10|     F|       2021-10-10|
|David|null| 45000|2020-03-05|     M|       2020-04-04|
+-----+----+------+----------+------+-----------------+

6. Aggregation with expr()
# Calculate average salary by gender
df_agg = df.groupBy("gender").agg(expr("AVG(salary) AS avg_salary"))
df_agg.show()
+------+----------+
|gender|avg_salary|
+------+----------+
|     F|   62500.0|
|     M|   52500.0|
+------+----------+

7. Using expr() in Filters
# Filter employees with salary > 55000
df_filter = df.where(expr("salary > 55000"))
df_filter.show()
+-----+---+------+----------+------+
| name|age|salary| join_date|gender|
+-----+---+------+----------+------+
|  Bob| 30| 60000|2022-06-20|     M|
|Cathy| 28| 75000|2021-09-10|     F|
+-----+---+------+----------+------+

8. Mathematical Functions
# Round salary to nearest 1000
df_math = df.withColumn("salary_rounded", expr("ROUND(salary, -3)"))
df_math.show()
+-----+----+------+----------+------+--------------+
| name| age|salary| join_date|gender|salary_rounded|
+-----+----+------+----------+------+--------------+
|Alice|  25| 50000|2023-01-15|     F|       50000.0|
|  Bob|  30| 60000|2022-06-20|     M|       60000.0|
|Cathy|  28| 75000|2021-09-10|     F|       75000.0|
|David|null| 45000|2020-03-05|     M|       45000.0|
+-----+----+------+----------+------+--------------+

9. Array and Struct Operations
# Create an array column and extract first element
df_array = df.withColumn("name_array", expr("ARRAY(name, gender)"))
df_array = df_array.withColumn("first_element", expr("name_array[0]"))
df_array.show()
+-----+----+------+----------+------+------------+-------------+
| name| age|salary| join_date|gender|  name_array|first_element|
+-----+----+------+----------+------+------------+-------------+
|Alice|  25| 50000|2023-01-15|     F|[Alice, F]  |        Alice|
|  Bob|  30| 60000|2022-06-20|     M|[Bob, M]    |          Bob|
|Cathy|  28| 75000|2021-09-10|     F|[Cathy, F]  |        Cathy|
|David|null| 45000|2020-03-05|     M|[David, M]  |        David|
+-----+----+------+----------+------+------------+-------------+

10. Combining Multiple Operations
Combine multiple SQL operations in a single expr().

# Combine arithmetic, conditional, and string operations
df_combined = df.withColumn(
    "summary",
    expr("""
        CONCAT(
            name, 
            ' has salary ', 
            CASE 
                WHEN salary > 70000 THEN 'High'
                ELSE 'Normal'
            END,
            ' and age ', 
            COALESCE(age, 0)
        )
    """)
)
df_combined.show(truncate=False)
+-----+----+------+----------+------+--------------------------------+
|name |age |salary|join_date |gender|summary                         |
+-----+----+------+----------+------+--------------------------------+
|Alice|25  |50000 |2023-01-15|F     |Alice has salary Normal and age 25|
|Bob  |30  |60000 |2022-06-20|M     |Bob has salary Normal and age 30  |
|Cathy|28  |75000 |2021-09-10|F     |Cathy has salary High and age 28  |
|David|null|45000 |2020-03-05|M     |David has salary Normal and age 0 |
+-----+----+------+----------+------+--------------------------------+

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> SELECTEXPR
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Differences Between expr() and selectExpr()
expr(): Returns a Column object for use in withColumn(), filter(), or other operations.
selectExpr(): Directly selects or transforms columns into a new DataFrame, replacing the need for select(expr(...)).
Example equivalence: df.select(expr("salary * 1.10 AS salary_with_bonus")) is the same as df.selectExpr("salary * 1.10 AS salary_with_bonus").

---

# Select name and salary, rename salary to employee_salary
df_select = df.selectExpr("name", "salary AS employee_salary")
df_select.show()

---
# Calculate salary with a 10% bonus
df_arithmetic = df.selectExpr("*", "salary * 1.10 AS salary_with_bonus")
df_arithmetic.show()
---
# Concatenate name and gender, convert name to uppercase
df_string = df.selectExpr(
    "UPPER(name) AS name_upper",
    "CONCAT(name, ' - ', gender) AS name_gender"
)
df_string.show()
---
# Categorize salary into Low, Medium, High
df_conditional = df.selectExpr(
    "name",
    "salary",
    "CASE WHEN salary < 50000 THEN 'Low' " +
    "WHEN salary BETWEEN 50000 AND 70000 THEN 'Medium' " +
    "ELSE 'High' END AS salary_category"
)
df_conditional.show()
---
# Replace null age with 27
df_null = df.selectExpr("*", "COALESCE(age, 27) AS age_filled")
df_null.show()
---
# Add 30 days to join_date and extract year
df_date = df.selectExpr(
    "name",
    "join_date",
    "DATE_ADD(join_date, 30) AS join_date_plus_30",
    "YEAR(join_date) AS join_year"
)
df_date.show()
---
# Round salary to nearest 1000
df_math = df.selectExpr("name", "salary", "ROUND(salary, -3) AS salary_rounded")
df_math.show()
---
# Create an array and extract first element
df_array = df.selectExpr(
    "name",
    "ARRAY(name, gender) AS name_gender_array",
    "ARRAY(name, gender)[0] AS first_element"
)
df_array.show()
---
# Combine arithmetic, conditional, and string operations
df_combined = df.selectExpr(
    "name",
    "CONCAT(name, ' has salary ', " +
    "CASE WHEN salary > 70000 THEN 'High' ELSE 'Normal' END, " +
    "' and age ', COALESCE(age, 0)) AS summary"
)
df_combined.show(truncate=False)
---
# Calculate average salary by gender
df_agg = df.groupBy("gender").selectExpr("gender", "AVG(salary) AS avg_salary")
df_agg.show()
---

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> COLUMN, ARRAY
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, col, concat_ws

# Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data
data = [
    (101, 87),
    (102, 90),
    (101, 78),
    (103, 88),
    (102, 85),
]

# Create DataFrame
df = spark.createDataFrame(data, ["StudentID", "Marks"])

# Group and collect marks into comma-separated string
result = df.groupBy("StudentID") \
    .agg(concat_ws(",", collect_list(col("Marks"))).alias("Marks_List"))

result.show()
+---------+----------+
|StudentID|Marks_List|
+---------+----------+
|      101|   87,78  |
|      102|   90,85  |
|      103|     88   |
+---------+----------+

---
from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, col, collect_list, map_from_entries, to_json

# Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data
data = [
    (101, "Hindi", 87),
    (101, "English", 78),
    (101, "Math", 65),
    (102, "Hindi", 90),
    (102, "English", 85),
]

# Create DataFrame
df = spark.createDataFrame(data, ["StudentID", "Subject", "Marks"])

# Group and build JSON dictionary
result = df.groupBy("StudentID").agg(
    to_json(
        map_from_entries(
            collect_list(
                struct(col("Subject"), col("Marks"))
            )
        )
    ).alias("Marks_Dict")
)

result.show(truncate=False)
+---------+------------------------------------------+
|StudentID|Marks_Dict                                |
+---------+------------------------------------------+
|101      |{"Hindi":87,"English":78,"Math":65}       |
|102      |{"Hindi":90,"English":85}                 |
+---------+------------------------------------------+

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> SIMPLE IF
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

spark = SparkSession.builder.getOrCreate()

# Sample data
data = [(1, 95), (2, 82), (3, 73), (4, 65), (5, 40)]
df = spark.createDataFrame(data, ["StudentID", "Marks"])

# Apply complex if-elif-else condition
df = df.withColumn("Grade",
    when(col("Marks") >= 90, "A+")
    .when(col("Marks") >= 80, "A")
    .when(col("Marks") >= 70, "B")
    .when(col("Marks") >= 60, "C")
    .otherwise("Fail")
)

df.show()
+----------+-----+-----+
|StudentID |Marks|Grade|
+----------+-----+-----+
|    1     |  95 | A+  |
|    2     |  82 | A   |
|    3     |  73 | B   |
|    4     |  65 | C   |
|    5     |  40 | Fail|
+----------+-----+-----+
> NESTED IF 
Senerio:
if Marks > 80:
    if Subject == "Math":
        if StudentID < 5:
            Remark = "Top Math Student"
        else:
            Remark = "Senior Math Star"
    else:
        Remark = "Excellent Non-Math Student"
else:
    Remark = "Needs Improvement"

Pyspark:
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "Math", 85),
    (2, "Math", 92),
    (5, "Math", 95),
    (3, "Science", 88),
    (4, "English", 75),
    (6, "Science", 60),
]

df = spark.createDataFrame(data, ["StudentID", "Subject", "Marks"])

# Deep nested if-else using when
df = df.withColumn("Remark",
    when(col("Marks") > 80,
         when(col("Subject") == "Math",
              when(col("StudentID") < 5, "Top Math Student")
              .otherwise("Senior Math Star")
         ).otherwise("Excellent Non-Math Student")
    ).otherwise("Needs Improvement")
)

df.show(truncate=False)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> FILTER
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Initialize Spark session
spark = SparkSession.builder.appName("ComplexFilterExamples").getOrCreate()

# Sample data
data = [
    ("Alice", 25, 50000, "2023-01-15", "F"),
    ("Bob", 30, 60000, "2022-06-20", "M"),
    ("Cathy", 28, 75000, "2021-09-10", "F"),
    ("David", None, 45000, "2020-03-05", "M")
]
columns = ["name", "age", "salary", "join_date", "gender"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
df.show()
+-----+----+------+----------+------+
| name| age|salary| join_date|gender|
+-----+----+------+----------+------+
|Alice|  25| 50000|2023-01-15|     F|
|  Bob|  30| 60000|2022-06-20|     M|
|Cathy|  28| 75000|2021-09-10|     F|
|David|null| 45000|2020-03-05|     M|
+-----+----+------+----------+------+
---

# Using expr() with filter()
df_filter1 = df.filter(
    expr("(gender = 'F' AND salary > 55000) OR (age < 30 AND join_date > '2021-01-01')")
)
df_filter1.show()
+-----+---+------+----------+------+
| name|age|salary| join_date|gender|
+-----+---+------+----------+------+
|Alice| 25| 50000|2023-01-15|     F|
|Cathy| 28| 75000|2021-09-10|     F|
+-----+---+------+----------+------+
Explanation:

(gender = 'F' AND salary > 55000): Matches Cathy (female, salary 75000).
(age < 30 AND join_date > '2021-01-01'): Matches Alice (age 25, joined 2023) and Cathy (age 28, joined 2021).
The OR combines these, and the result includes both Alice and Cathy.
Native PySpark Equivalent:

from pyspark.sql.functions import col
df_filter1_native = df.filter(
    ((col("gender") == "F") & (col("salary") > 55000)) |
    ((col("age") < 30) & (col("join_date") > "2021-01-01"))
)
df_filter1_native.show()

2. Using CASE WHEN in selectExpr() with Filtering
Use selectExpr() to create a new column based on complex conditions and then filter rows based on that column.
# Create a category column and filter rows where category is 'HighValue'
df_filter2 = df.selectExpr(
    "*",
    "CASE WHEN salary > 60000 OR (age IS NOT NULL AND age >= 28) THEN 'HighValue' ELSE 'Standard' END AS employee_category"
).filter(expr("employee_category = 'HighValue'"))
df_filter2.show()
+-----+---+------+----------+------+----------------+
| name|age|salary| join_date|gender|employee_category|
+-----+---+------+----------+------+----------------+
|  Bob| 30| 60000|2022-06-20|     M|       HighValue|
|Cathy| 28| 75000|2021-09-10|     F|       HighValue|
+-----+---+------+----------+------+----------------+
Explanation:

selectExpr() creates a new column employee_category:
HighValue if salary > 60000 or age is not null and >= 28.
Standard otherwise.
The filter() selects rows where employee_category = 'HighValue', matching Bob (age 30) and Cathy (salary 75000, age 28).

3. Pattern Matching with LIKE or RLIKE
Filter rows where the name contains a specific pattern or matches a regular expression.
# Filter names starting with 'A' or containing 'th' and salary >= 50000
df_filter3 = df.filter(
    expr("name LIKE 'A%' OR name RLIKE '.*th.*' AND salary >= 50000")
)
df_filter3.show()
+-----+---+------+----------+------+
| name|age|salary| join_date|gender|
+-----+---+------+----------+------+
|Alice| 25| 50000|2023-01-15|     F|
|Cathy| 28| 75000|2021-09-10|     F|
+-----+---+------+----------+------+
Explanation:

name LIKE 'A%': Matches names starting with 'A' (Alice).
name RLIKE '.*th.*': Matches names containing 'th' (Cathy).
AND salary >= 50000: Ensures salary is at least 50000.
The OR combines the name conditions, and the AND applies the salary condition.
Native PySpark Equivalent:

df_filter3_native = df.filter(
    (col("name").like("A%") | col("name").rlike(".*th.*")) & (col("salary") >= 50000)
)
df_filter3_native.show()

4. Date-Based Filtering with Complex Logic
Filter rows based on date conditions, such as employees who joined within a specific date range or have a specific tenure.

# Filter employees who joined between 2021 and 2023 and are younger than 30 or have null age
df_filter4 = df.filter(
    expr("join_date BETWEEN '2021-01-01' AND '2023-12-31' AND (age < 30 OR age IS NULL)")
)
df_filter4.show()
+-----+---+------+----------+------+
| name|age|salary| join_date|gender|
+-----+---+------+----------+------+
|Alice| 25| 50000|2023-01-15|     F|
|Cathy| 28| 75000|2021-09-10|     F|
+-----+---+------+----------+------+
Explanation:

join_date BETWEEN '2021-01-01' AND '2023-12-31': Matches Alice (2023-01-15) and Cathy (2021-09-10).
(age < 30 OR age IS NULL): Matches Alice (age 25), Cathy (age 28), and excludes David (null age, but joined in 2020).

5. Nested Conditions with Null Handling
Filter rows with nested conditions, handling null values explicitly.
# Filter where age is null or (salary > 50000 and gender is 'M')
df_filter5 = df.filter(
    expr("age IS NULL OR (salary > 50000 AND gender = 'M')")
)
df_filter5.show()
+-----+----+------+----------+------+
| name| age|salary| join_date|gender|
+-----+----+------+----------+------+
|  Bob|  30| 60000|2022-06-20|     M|
|David|null| 45000|2020-03-05|     M|
+-----+----+------+----------+------+
Explanation:

age IS NULL: Matches David.
(salary > 50000 AND gender = 'M'): Matches Bob.
The OR combines these, including both Bob and David.

6. Combining selectExpr() with Complex Filtering
Use selectExpr() to create derived columns and then apply a complex filter.
# Create a tenure column and filter employees with tenure > 1 year and high salary or young age
df_filter6 = df.selectExpr(
    "*",
    "DATEDIFF(CURRENT_DATE, join_date) / 365 AS tenure_years"
).filter(
    expr("tenure_years > 1 AND (salary > 60000 OR age < 28)")
)
df_filter6.show()
+-----+---+------+----------+------+------------------+
| name|age|salary| join_date|gender|      tenure_years|
+-----+---+------+----------+------+------------------+
|Cathy| 28| 75000|2021-09-10|     F|3.857534246575342|
+-----+---+------+----------+------+------------------+
Explanation:

DATEDIFF(CURRENT_DATE, join_date) / 365: Calculates tenure in years.
tenure_years > 1: Filters employees with more than 1 year of tenure (excludes Alice, Bob, includes Cathy, David).
(salary > 60000 OR age < 28): Matches Cathy (salary 75000) but not David (age null, salary 45000).

7. Filtering with Array and Struct Operations
Filter based on complex types like arrays.
# Create an array and filter based on array conditions
df_filter7 = df.selectExpr(
    "*",
    "ARRAY(name, gender) AS name_gender_array"
).filter(
    expr("name_gender_array[1] = 'F' AND salary >= 50000")
)
df_filter7.show()
+-----+----+------+----------+------+-----------------+
| name| age|salary| join_date|gender|name_gender_array|
+-----+----+------+----------+------+-----------------+
|Alice|  25| 50000|2023-01-15|     F|     [Alice, F]  |
|Cathy|  28| 75000|2021-09-10|     F|     [Cathy, F]  |
+-----+----+------+----------+------+-----------------+

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> WITHCOLUMNS, FILTER
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# Using withColumns to add multiple columns
df_withColumns_simple = df.withColumns({
    "salary_with_bonus": exprprud expr("salary * 1.10"),
    "current_date": lit("2025-07-20")
})
df_withColumns_simple.show()

> EXPR
# Using withColumn with a complex SQL expression
df_withColumn_complex = df.withColumn(
    "employee_summary",
    expr("""
        CONCAT(
            name, ' is ',
            CASE 
                WHEN salary > 60000 THEN 'high-paid'
                WHEN salary BETWEEN 50000 AND 60000 THEN 'medium-paid'
                ELSE 'low-paid'
            END,
            ' with tenure ',
            ROUND(DATEDIFF('2025-07-20', join_date) / 365, 1),
            ' years'
        )
    """)
).filter(expr("employee_summary LIKE '%high-paid%' OR age IS NULL"))
df_withColumn_complex.show(truncate=False)

---

# Using withColumns with multiple complex expressions
df_withColumns_complex = df.withColumns({
    "salary_category": expr("""
        CASE 
            WHEN salary > 60000 THEN 'High'
            WHEN salary BETWEEN 50000 AND 60000 THEN 'Medium'
            ELSE 'Low'
        END
    """),
    "tenure_years": expr("ROUND(DATEDIFF('2025-07-20', join_date) / 365, 1)"),
    "name_gender_array": expr("ARRAY(name, gender)"),
    "age_filled": expr("COALESCE(age, 27)")
}).filter(expr("salary_category = 'High' OR tenure_years > 4"))
df_withColumns_complex.show(truncate=False)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> EXPR SELECTEXPR
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

expr()	
Definition	A function that parses a SQL expression string and returns a Column object for use in DataFrame operations like withColumn(), filter(), or groupBy().	

selectExpr()
A DataFrame method that evaluates one or more SQL expressions and returns a new DataFrame with the results as columns.

# Using expr() with withColumn to add a single column
df_expr_simple = df.withColumn("salary_with_bonus", expr("salary * 1.10"))
df_expr_simple.show()

# Using selectExpr to select and transform columns
df_selectExpr_simple = df.selectExpr("name", "salary * 1.10 AS salary_with_bonus")
df_selectExpr_simple.show()
---

# Using selectExpr for complex column transformations, followed by filtering
df_selectExpr_complex = df.selectExpr(
    "name",
    "salary",
    "CASE WHEN salary > 60000 THEN 'High' WHEN salary BETWEEN 50000 AND 60000 THEN 'Medium' ELSE 'Low' END AS salary_category",
    "ROUND(DATEDIFF('2025-07-20', join_date) / 365, 1) AS tenure_years",
    "ARRAY(name, gender) AS name_gender_array"
).filter(
    expr("salary_category = 'High' OR tenure_years > 4")
)
df_selectExpr_complex.show(truncate=False)

---
# Using expr() for complex transformations and filtering
df_expr_complex = df.withColumn(
    "employee_summary",
    expr("""
        CONCAT(
            name, ' is ',
            CASE 
                WHEN salary > 60000 THEN 'high-paid'
                WHEN salary BETWEEN 50000 AND 60000 THEN 'medium-paid'
                ELSE 'low-paid'
            END,
            ' with tenure ',
            ROUND(DATEDIFF('2025-07-20', join_date) / 365, 1),
            ' years'
        )
    """)
).filter(
    expr("employee_summary LIKE '%high-paid%' OR age IS NULL")
).withColumn(
    "age_filled",
    expr("COALESCE(age, 27)")
)
df_expr_complex.show(truncate=False)

> KEY

data = [
    ("Alice", 25, 50000, "2023-01-15", "F"),
    ("David", None, 45000, "2020-03-05", "M")
]
+-----+----+------+----------+------+
| name| age|salary| join_date|gender|
+-----+----+------+----------+------+
|Alice|  25| 50000|2023-01-15|     F|
|David|null| 45000|2020-03-05|     M|
+-----+----+------+----------+------+

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> ORDERBY
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

df_orderBy_simple = df.orderBy("salary")
df_orderBy_simple.show()

#####

df.orderBy(col("gender"), col("salary").desc())

#####

df_ordered = df.orderBy(
    col("gender").asc(),                        # sort gender A-Z (F first, then M)
    col("salary").desc(),                       # then by salary high to low
    (col("salary") * col("experience")).desc()  # then by total cost-to-company (computed)
)

df_ordered.show()
+-----+------+------+----------+
| name|gender|salary|experience|
+-----+------+------+----------+
|  Eve|     F|  1500|         1|
|Alice|     F|  1000|         3|
|Carol|     F|  1000|         5|
|  Bob|     M|  1500|         2|
|David|     M|  1200|         4|
+-----+------+------+----------+

#####

# Using orderBy with expr() and selectExpr
df_orderBy_complex = df.selectExpr(
    "*",
    "ROUND(DATEDIFF('2025-07-20', join_date) / 365, 1) AS tenure_years",
    "CASE WHEN salary > 60000 THEN 'High' WHEN salary BETWEEN 50000 AND 60000 THEN 'Medium' ELSE 'Low' END AS salary_category"
).orderBy(
    expr("tenure_years DESC"),
    expr("salary_category ASC")
)
df_orderBy_complex.show(truncate=False)

# Using sort with expr() and selectExpr
df_sort_complex = df.selectExpr(
    "*",
    "ROUND(DATEDIFF('2025-07-20', join_date) / 365, 1) AS tenure_years",
    "CASE WHEN salary > 60000 THEN 'High' WHEN salary BETWEEN 50000 AND 60000 THEN 'Medium' ELSE 'Low' END AS salary_category"
).sort(
    expr("tenure_years DESC"),
    expr("salary_category ASC")
)
df_sort_complex.show(truncate=False)

---

# Using orderBy with expr() for conditional sorting
df_orderBy_null = df.selectExpr(
    "*",
    "ROUND(DATEDIFF('2025-07-20', join_date) / 365, 1) AS tenure_years"
).withColumn(
    "salary_per_year",
    expr("ROUND(salary / tenure_years, 2)")
).orderBy(
    expr("CASE WHEN age IS NULL THEN 1 ELSE 0 END"),  # Null ages come last
    expr("salary_per_year DESC")
).filter(expr("tenure_years > 2"))
df_orderBy_null.show(truncate=False)


# Using sort with expr() for conditional sorting
df_sort_null = df.selectExpr(
    "*",
    "ROUND(DATEDIFF('2025-07-20', join_date) / 365, 1) AS tenure_years"
).withColumn(
    "salary_per_year",
    expr("ROUND(salary / tenure_years, 2)")
).sort(
    expr("CASE WHEN age IS NULL THEN 1 ELSE 0 END"),  # Null ages come last
    expr("salary_per_year DESC")
).filter(expr("tenure_years > 2"))
df_sort_null.show(truncate=False)

#####

df.orderBy("gender", col("salary").desc())
"gender" is passed as a string — PySpark automatically treats it as a column.

col("salary").desc() is a Column expression — used to specify descending order.


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> WINDOW 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Window Specification: Defined using Window.partitionBy() (grouping) and Window.orderBy() (ordering within the partition). Optional rowsBetween() or rangeBetween() defines the window frame.
Types of Window Functions:
Ranking Functions: row_number(), rank(), dense_rank(), ntile(), percent_rank().
Aggregate Functions: sum(), avg(), min(), max(), etc., over a window.
Value Functions: lag(), lead(), first(), last().
Analytic Functions: cume_dist(), stddev(), etc.
Usage: Applied using over(window) with a column expression, often via withColumn() or expr().

---

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, rank, dense_rank, sum, avg, lag, lead, max, min
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

data = [
    ("Sales", "Alice", 2024, 1000),
    ("Sales", "Bob",   2024, 1200),
    ("Sales", "Alice", 2025, 1500),
    ("HR",    "David", 2024, 1100),
    ("HR",    "Eve",   2025, 1300),
    ("HR",    "Eve",   2024, 1400),
    ("HR",    "David", 2025, 1250),
]

df = spark.createDataFrame(data, ["dept", "employee", "year", "salary"])
df.show()

---

windowSpec = Window.orderBy("salary")
df.withColumn("row_num", row_number().over(windowSpec)).show()

---

df.withColumn("rank", rank().over(windowSpec)).show()
df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> WINDOW, RANK, DENSE_RANK
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++


from pyspark.sql.functions import least, greatest

df = spark.createDataFrame([
    ("A", 10, 20, 30),
    ("B", 50, 40, 60),
    ("C", 5, 25, 15)
], ["name", "val1", "val2", "val3"])

df = df.withColumn("min_val", least("val1", "val2", "val3")) \
       .withColumn("max_val", greatest("val1", "val2", "val3"))

df.show()
+-----+-----+-----+-----+--------+--------+
|name |val1 |val2 |val3 |min_val |max_val |
+-----+-----+-----+-----+--------+--------+
|A    |10   |20   |30   |10      |30      |
|B    |50   |40   |60   |40      |60      |
|C    |5    |25   |15   |5       |25      |
+-----+-----+-----+-----+--------+--------+

---

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("RankVsDenseRank").getOrCreate()

# Sample data with tied salaries
data = [
    ("Alice", 25, 50000, "2023-01-15", "F", "HR"),
    ("Bob", 30, 60000, "2022-06-20", "M", "IT"),
    ("Cathy", 28, 60000, "2021-09-10", "F", "HR"),
    ("David", None, 45000, "2020-03-05", "M", "IT"),
    ("Eve", 27, 50000, "2022-12-01", "F", "Finance"),
    ("Frank", 32, 80000, "2021-03-15", "M", "Finance")
]
columns = ["name", "age", "salary", "join_date", "gender", "department"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
df.show(truncate=False)
+-----+----+------+----------+------+----------+
|name |age |salary|join_date |gender|department|
+-----+----+------+----------+------+----------+
|Alice|25  |50000 |2023-01-15|F     |HR        |
|Bob  |30  |60000 |2022-06-20|M     |IT        |
|Cathy|28  |60000 |2021-09-10|F     |HR        |
|David|null|45000 |2020-03-05|M     |IT        |
|Eve  |27  |50000 |2022-12-01|F     |Finance   |
|Frank|32  |80000 |2021-03-15|M     |Finance   |
+-----+----+------+----------+------+----------+

# Define window: order by salary (no partitioning)
window_spec = Window.orderBy(col("salary").desc())

# Add RANK and DENSE_RANK
df_medium = df.selectExpr(
    "name",
    "salary",
    "RANK() OVER (ORDER BY salary DESC) AS rank",
    "DENSE_RANK() OVER (ORDER BY salary DESC) AS dense_rank"
).orderBy(col("salary").desc())
df_medium.show(truncate=False)

+-----+------+----+----------+
|name |salary|rank|dense_rank|
+-----+------+----+----------+
|Frank|80000 |1   |1         |
|Bob  |60000 |2   |2         |
|Cathy|60000 |2   |2         |
|Alice|50000 |4   |3         |
|Eve  |50000 |4   |3         |
|David|45000 |6   |4         |
+-----+------+----+----------+

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> LOG, LOGGER, AWS LOGGER
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("LogWithCounter").getOrCreate()

log_path = "s3a://your-bucket/logs/"  # or "/tmp/logs"
columns = ["log_id", "timestamp", "status", "message"]

# Counter (global)
log_counter = 0
is_first = True  # To write header only once

def log_event(status, message):
    global log_counter, is_first

    log_counter =  log_counter + 1
    row = [(log_counter, datetime.now().isoformat(), status, message)]
    df = spark.createDataFrame(row, schema=columns)
    df.write.csv(log_path, mode="append", header=is_first)
    is_first = False

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> RDD 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark create RDD example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.sparkContext\
    .parallelize([
        (1, 2, 3, 'a b c'),
        (4, 5, 6, 'd e f'),
        (7, 8, 9, 'g h i')
    ])\
    .toDF(['col1', 'col2', 'col3', 'col4'])

df.show()

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> IF, ELSE, CONDITION, WHEN, OTHERWISE 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

If person is an Indian citizen:
    If person belongs to any Indian state:
        If age ≥ 18:
            If category is SC or ST:
                If state is "Uttar Pradesh" or "Bihar":
                    benefit_amount = 25000
                Else:
                    benefit_amount = 20000
            Else:
                benefit_amount = 10000
        Else:
            If category is OBC or category is SC:
                benefit_amount = 5000
            Else:
                benefit_amount = 0
    Else:
        If category is SC or ST:
            benefit_amount = 8000
        Else:
            benefit_amount = 0
Else:
    If state is in India and category is SC or ST:
        benefit_amount = 12000
    Else:
        If country is "USA":
            benefit_amount = 100
        ELIf country is "Greenland":
            benefit_amount = 200
        ELIf country is "China":
            benefit_amount = 300
        ELIf country is "Russia":
            benefit_amount = 500

---

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("BenefitCalculation").getOrCreate()

data = [
    ("Indian", "Uttar Pradesh", 19, "SC", "India"),
    ("Indian", "Bihar", 20, "ST", "India"),
    ("Indian", "Kerala", 22, "GEN", "India"),
    ("Indian", "Tamil Nadu", 16, "OBC", "India"),
    ("Indian", "Rajasthan", 17, "SC", "India"),
    ("Indian", "Maharashtra", 15, "GEN", "India"),
    ("Indian", "Uttar Pradesh", 30, "GEN", "India"),
    ("Indian", "Delhi", 25, "OBC", "India"),
    ("Indian", "Haryana", 40, "ST", "India"),
    ("Indian", "Unknown", 19, "SC", "India"),
    ("Indian", "Unknown", 15, "GEN", "India"),
    ("Foreigner", "Delhi", 21, "SC", "India"),
    ("Foreigner", "Mumbai", 22, "GEN", "India"),
    ("Foreigner", "N/A", 30, "GEN", "USA"),
    ("Foreigner", "N/A", 28, "GEN", "Greenland"),
    ("Foreigner", "N/A", 29, "GEN", "China"),
    ("Foreigner", "N/A", 31, "GEN", "Russia"),
    ("Foreigner", "N/A", 33, "GEN", "UK"),
    ("Foreigner", "Bihar", 26, "ST", "India"),
    ("Foreigner", "Kolkata", 25, "SC", "India"),
    ("Indian", "Punjab", 16, "SC", "India"),
    ("Indian", "Bihar", 16, "GEN", "India"),
    ("Indian", "Unknown", 18, "GEN", "India"),
    ("Indian", "Jharkhand", 20, "ST", "India"),
]

columns = ["citizenship", "state", "age", "category", "country"]
df = spark.createDataFrame(data, columns)

indian_states = ["Uttar Pradesh", "Bihar", "Kerala", "Tamil Nadu", "Delhi", "Maharashtra", "Rajasthan", "Punjab", "Haryana", "Jharkhand"]

df = df.withColumn(
    "benefit_amount",
    when(col("citizenship") == "Indian",
         when(col("state").isin(indian_states),
              when(col("age") >= 18,
                   when(col("category").isin("SC", "ST"),
                        when(col("state").isin("Uttar Pradesh", "Bihar"), 25000)
                        .otherwise(20000))
                   .otherwise(10000))
              .otherwise(
                  when(col("category").isin("SC", "OBC"), 5000)
                  .otherwise(0))
              )
         .otherwise(
             when(col("category").isin("SC", "ST"), 8000)
             .otherwise(0))
         )
    .otherwise(
        when((col("state").isin(indian_states)) & (col("category").isin("SC", "ST")), 12000)
        .otherwise(
            when(col("country") == "USA", 100)
            .when(col("country") == "Greenland", 200)
            .when(col("country") == "China", 300)
            .when(col("country") == "Russia", 500)
            .otherwise(0)
        )
    )
)

df.show(truncate=False)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> LOG, LOGGER, LOGGER IN S3 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import lit, current_timestamp

def write2log(spark, action_type, message, activity_name, s3_path):
    mode = "overwrite" if action_type == "INIT" else "append"

    schema = StructType([
        StructField("activity_name", StringType(), True),
        StructField("action_type", StringType(), True),
        StructField("message", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    log_df = spark.createDataFrame(
        [(activity_name, action_type, message)],
        schema=schema
    ).withColumn("timestamp", current_timestamp())

    log_df.write.mode(mode).format("csv").option("header", True).save(s3_path)

=========================================================

write2log(
    spark,
    action_type="INIT",
    message="Pipeline started",
    activity_name="load_vehicle_data",
    s3_path="s3://my-bucket/logs/vehicle_pipeline/"
)
"

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> DATA
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> SETUP, CLI
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

pip install databricks-cli --upgrade

databricks --version

#####

setx DATABRICKS_CONFIG_FILE "<path-to-file>" /M

Console

[DEFAULT]
host = <workspace-URL>
username = <username>
password = <password>

#####

Config file details 
    Location : c:\User\<username>
    File : .databrickscfg

Contents of config file
[DEFAULT]
host : https://adb-5767539265336440.0.azuredatabricks.net/
token
 
#####

 c:\>databricks workspace ls

#####

C:\>databricks --version
Databricks CLI v0.209.0

#####

C:\>databricks configure
Databricks Host: https://adb-6803681728933823.3.azuredatabricks.net/
Personal Access Token: ************************************

#####

C:\>databricks clusters --help

C:\>databricks clusters list
ID                               Name                                                                       State
1107-233031-yeea6e6x  dlt-execution-f25e0353-cfbd-41cb-9710-23d9a26a42e8    RUNNING
0626-043729-ux0onm48  cl-dev-shared                                                            TERMINATED
      

C:\>databricks clusters get <id>

C:\>databricks clusters get 0711-002700-17gue8wz

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> CATALOG
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

The two-level namespace with Unity Catalog three-level namespace notation consisting of a catalog, schema, and table or view

catalog -> database -> table

#####

select * from catalog.database.table

##### Create Catalog & Permission


CREATE CATALOG IF NOT EXISTS demo_catalog;

GRANT USAGE ON CATALOG demo_catalog;

USE CATALOG demo_catalog;

CREATE DATABASE IF NOT EXISTS demo_database;

GRANT USAGE, CREATE ON DATABASE demo_database;


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> USE, TABLES, ABSOLUTE PATH
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

%sql
USE CATALOG sandbox;
USE SCHEMA sos;
select * from emp
limit 10

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> Use, Tables, relative path
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

%sql
select * from sandbox.sos.emp
limit 10

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "3", "key" : [ "AUTO", "AUTOLOADER" ] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

trigger.once()
    only one time it will trigger and close the streaming like Batch processing

File_Notification_Service
   When there is a lot of file arriving in source loction for processing

Note
    need to create a checkpoint folder

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> STREAMING, JSONE
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

val df = spark.readStream
     .format("cloudFiles")
     .option("cloudFiles.format", "json")
     .load("/input/path")

df.writeStream.trigger(Trigger.Once)
   .format("delta")
   .start("/output/path")

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> IMAGE FILE PROCESSING
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

spark.readStream.format("cloudFiles") 
  .option("cloudFiles.format", "binaryFile") 
  .load("<path_to_source_data>") 
  .writeStream 
  .option("checkpointLocation", "<path_to_checkpoint>") 
  .start("<path_to_target")

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> AUTOLOADER
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# Import functions
from pyspark.sql.functions import col, current_timestamp

# Define variables used in code below
file_path = "/databricks-datasets/structured-streaming/events"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{username}_etl_quickstart"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"

# Clear out data from previous demo execution
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest JSON data to a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name))


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> TRIGGER
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Default trigger (runs micro-batch as soon as it can)
df.writeStream \
  .format("console") \
  .start()

ProcessingTime trigger with two-seconds micro-batch interval
df.writeStream \
  .format("console") \
  .trigger(processingTime='2 seconds') \
  .start()

One-time trigger
df.writeStream \
  .format("console") \
  .trigger(once=True) \
  .start()

Continuous trigger with one-second checkpointing interval
df.writeStream
  .format("console")
  .trigger(continuous='1 second')
  .start()

######

spark.conf.set("spark.sql.shuffle.partitions", "2")

# View stream in real-time
query = (
    streamingActionCountsDF
        .writeStream
        .format("memory")
        .queryName("counts")
        .outputMode("complete")
        .start()
)

while not query.isActive:
    time.sleep(1)

while query.isActive:
    for progress in query.recentProgress:

        if "numInputRows" in progress:
            print("Loaded {} rows from file {}"
                  .format(progress["numInputRows"], progress["numInputRows"]))
    time.sleep(1)
 
# query.awaitTermination()

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> JSON, WRITE, UNITY CATALOG, TABLE
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .schema(schema) 
  .load("abfss://raw@sttpldatasolutionsdev.dfs.core.windows.net/sos/autoloader_data")
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  #.trigger(availableNow=True)
  .toTable("sandbox.sos.emp"))

---

# Import functions
from pyspark.sql.functions import col, current_timestamp

# Define variables used in code below
file_path = "/databricks-datasets/structured-streaming/events"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{username}_etl_quickstart"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"

# Clear out data from previous demo execution
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest JSON data to a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name))

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> CACHE TABLE, CACHE
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

OPTIONS clause with storageLevel key and value pair. A Warning is issued when a key other than storageLevel is used. 
The valid options for storageLevel are:
NONE
DISK_ONLY
DISK_ONLY_2
MEMORY_ONLY
MEMORY_ONLY_2
MEMORY_ONLY_SER
MEMORY_ONLY_SER_2
MEMORY_AND_DISK
MEMORY_AND_DISK_2
MEMORY_AND_DISK_SER
MEMORY_AND_DISK_SER_2
OFF_HEAP

#####

Example:
CACHE TABLE testCache OPTIONS ('storageLevel' 'DISK_ONLY') SELECT * FROM testData;
UNCACHE TABLE testCache;

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> CHART, GRAPH
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

import pyspark.pandas as ps
from pyspark.sql.functions import *

speed = [0.1, 17.5, 40, 48, 52, 69, 88]
lifespan = [2, 8, 70, 1.5, 25, 12, 28]
lifespan2 = [20, 80, 70, 1.5, 1, 1, 28]
index = ['snail', 'pig', 'elephant',
         'rabbit', 'giraffe', 'coyote', 'horse']


df = ps.DataFrame({'speed': speed,
                   'lifespan': lifespan, 'lifespan2': lifespan2}, index=index)
display(df)

df.plot.bar()

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> CHART
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 
import pyspark.pandas as ps
from pyspark.sql.functions import *

simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NV",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","DE",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","NV",80000,25,18000),
    ("Kumar","Marketing","NJ",91000,50,21000)
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
display(df)


# Convert pyspark.sql.dataframe.DataFrame to pyspark.pandas.frame.DataFrame
temp_df = ps.DataFrame(df).set_index('salary')

display(temp_df)
# Plot spark dataframe
# temp_df.column_name.plot.pie()
temp_df.plot.bar(x='employee_name', y='age')  

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> CLUSTER
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Databricks cluster is a set of computation resources and configurations on which you run data engineering, data science, and data 
analytics workloads, such as production ETL pipelines, streaming analytics, ad-hoc analytics, and machine learning

ALL-PURPOSE
You can create an all-purpose cluster using the UI, CLI, or REST API. You can manually terminate and restart an all-purpose cluster. 
Multiple users can share such clusters to do collaborative interactive analysis.

JOB CLUSTER
The Azure Databricks job scheduler creates a job cluster when you run a job on a new job cluster and terminates the cluster when 
the job is complete. You cannot restart a job cluster.


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 ENVIRONMENT VARIABLES
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

The goal is to the have environment variable, available in all notebooks executed on the cluster.

#####

cluster -> Advance setting -> Environment variables

Ex:
STORAGE_LANDING=abfss://landing@sttpldatasolutionsdev.dfs.core.windows.net
STORAGE_RAW=abfss://raw@sttpldatasolutionsdev.dfs.core.windows.net
STORAGE_MODELLED=abfss://modelled@sttpldatasolutionsdev.dfs.core.windows.net
STORAGE_PROCESSED=abfss://processed@sttpldatasolutionsdev.dfs.core.windows.net

export MY_TEST_VAR=test

#####

How to use environment variable in notebook
    import os
    landing_location = os.getenv('STORAGE_LANDING')
    print(landing_location)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "7", "key" : [ "JDBC", "CONNECTION"] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
MYSQL
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

df_mysql = spark.read.format(“jdbc”)
   .option(“url”, “jdbc:mysql://localhost:port/db”)
   .option(“driver”, “com.mysql.jdbc.Driver”)
   .option(“dbtable”, “tablename”) 
   .option(“user”, “user”) 
   .option(“password”, “password”) 
   .load()

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
DB2
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

DB2 table
Make sure you have DB2 library as a dependency in your pom.xml file or DB2 jars in your classpath.


val df_db2 = spark.read.format(“jdbc”)
   .option(“url”, “jdbc:db2://localhost:50000/dbname”)
   .option(“driver”, “com.ibm.db2.jcc.DB2Driver”)
   .option(“dbtable”, “tablename”) 
   .option(“user”, “user”) 
   .option(“password”, “password”) 
   .load()

#####

jdbcHostname = "ss-rajade.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "database-rajade"
jdbcUsername = "rajade"
jdbcPassword = "Test@1234"
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}: {jdbcPort}; databaseName={jdbcDatabase}; user={jdbcUsername}; password={jdbcPassword}"

df1 = spark.read.format("jdbc").option("url", jdbcUrl).option("dbtable", "Sales LT.Product").load() 
display (df1)

#####

connectionString = "jdbc:sqlserver://ss-rajade.database.windows.net:1433;database=database-rajade;user=rajade@ss-rajade; password= {Test@1234); encrypt=true; trustServerCertificate=false; hostNameInCertificate *.database.windows.net; loginTimeout=30;"

df spark.read.jdbc (connectionString, "SalesLT. Address")
display(df)

##### mysql + partation

val dataframe_mysql = spark.read. format ("jdbc"). option ("url", "jdbc:mysql://localhost/test").option ("driver", "com.mysql.jdbc.Driver").option ("dbtable", "t1").option ("user", "root").option ("password", "root").load()

val dataframe_mysql = spark.read.format("jdbc").option ("url",
"jdbc:mysql://localhost/test").option ("driver", "com.mysql.jdbc.Driver").option ("dbtable", "t1").option ("user", "root").option ("password", "root").option ("partitionColumn", "sno").option ("numPartitions", 2). option ("lowerBound", 0). option ("upperBound", 4).load()

#####

jdbcHostname = dbutils.secrets.get(scope = "akv_secret_demo", key = "jdbcHostname")
jdbcPort = 1433 
jdbcDatabase = dbutils.secrets.get(scope = "akv_secret_demo", key "jdbcDatabase") 
jdbcUsername = dbutils.secrets.get(scope = "akv_secret_demo", key "jdbcUsername") 
jdbcPassword = dbutils.secrets.get(scope = "akv_secret_demo", key "jdbcPassword")
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

jdbcUrl =  f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername}; password={jdbcPassword}"

empDF = spark.read.format("jdbc").option("url", jdbcUrl).option("dbtable", "dbo.emp").load()
display(empDF)

#####


container = dbutils.secrets.get(scope = "akv_secret_demo", key = "container")
storageaccount =dbutils.secrets.get(scope = "akv_secret_demo", key = "storageaccount")
accesskey = dbutils.secrets.get(scope = "akv_secret_demo", key = "accesskey")

dbutils. fs.mount (
    source = f"wasbs://{container}@{storageaccount} .blob.core.windows.net",
    mount_point = "/mnt/adls_demo",
    extra_configs = {f"fs.azure.account..key. {storageaccount}.blob.core.windows.net":f" {accesskey}"})
	
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "8", "key" : [ "CSV", "READ"] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
- Key
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

spark.read
spark.write
spark.readStream
spark.writeStream


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
- Read single csv file
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

file_location = "abfss://raw@sttpldatasolutionsdev.dfs.core.windows.net/sos/temp_files/csv_files/data_01.csv"
df = spark.read.options(header='True') \
    .options(delimiter=';') \
    .csv(path=file_location)


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
- Read multiple csv file
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

file_01 = "abfss://raw@sttpldatasolutionsdev.dfs.core.windows.net/sos/temp_files/csv_files/data_01.csv"
file_02 = "abfss://raw@sttpldatasolutionsdev.dfs.core.windows.net/sos/temp_files/csv_files/data_02.csv"

df = spark.read.options(header='True') \
    .options(delimiter=';') \
    .csv(path=[ file_01, file_02 ] )


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
- Read multiple csv files from a folder
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

file_location = "abfss://raw@sttpldatasolutionsdev.dfs.core.windows.net/sos/temp_files/csv_files/"
df = spark.read.options(header='True') \
    .options(delimiter=';') \
    .csv(path=file_location)
    

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
- Read csv file using load
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

file_01 = "abfss://raw@sttpldatasolutionsdev.dfs.core.windows.net/sos/temp_files/csv_files/data_01.csv"

df = spark.read.format("csv") \
    .option("header","true") \
    .options(delimiter=';') \
    .load(file_01) 


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
- Read csv using generate schema 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

file_01 = "abfss://raw@sttpldatasolutionsdev.dfs.core.windows.net/sos/temp_files/csv_files/data_00.csv"

df = spark.read.options(inferSchema='True') \
    .option(delimiter=',') \
    .options(header='True') \
    .csv(file_01)


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
- Read csv with schema specification
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
file_01 = "abfss://raw@sttpldatasolutionsdev.dfs.core.windows.net/sos/temp_files/csv_files/data_00.csv"

file_format = StructType() \
      .add("EMP_ID",IntegerType(),True) \
      .add("EMP_NAME",StringType(),True) \
      .add("EMP_EMAIL",StringType(),True) 

df = spark.read.options(header='True') \
    .options(delimiter=',') \
    .schema(file_format) \
    .csv(path=file_01)



+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
- Releated 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('PySpark Read CSV').getOrCreate()

df.printSchema()

display(df)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "9", "key" : [ "CSV", "WRITE" ] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

in_location = "abfss://raw@sttpldatasolutionsdev.dfs.core.windows.net/sos/temp_files/csv_files/data_00.csv"
out_location = "abfss://checkpoint@sttpldatasolutionsdev.dfs.core.windows.net/raw/sos/temp_file/emp"

df = spark.read.options(header='True') \
    .options(delimiter=',') \
    .csv(path=in_location)

df.write.option("header",True) \
    .format("csv") \
    .csv(out_location)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
- Write csv file from dataframe using save
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

df.write.option("header",True) \
    .format("csv") \
    .save(out_location)


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
- Write csv file from dataframe using save
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

df.write.option("header",True) \
    .format("csv") \
    .mode('overwrite') \
    .save(out_location)


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
- Write csv file IF file already exists
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

in_location = "abfss://raw@sttpldatasolutionsdev.dfs.core.windows.net/sos/temp_files/csv_files/data_00.csv"
out_location = "abfss://checkpoint@sttpldatasolutionsdev.dfs.core.windows.net/raw/sos/temp_file/emp"

df = spark.read.options(header='True') \
    .options(delimiter=',') \
    .csv(path=in_location)

df.write.option("header",True) \
    .format("csv") \
    .mode('overwrite') \
    .csv(out_location) 


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
- Releated 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('PySpark Read CSV').getOrCreate()

df.printSchema()

display(df)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "10", "key" : [ "EXTERNAL"] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

CREATE TABLE PO_LINES_ALL
USING parquet
OPTIONS (path "wasbs://landing-zone@stprdnaeus2.blob.core.windows.net/on-premises/oracle-r12/PROD/Contraloria/PO/PO_DISTRIBUTIONS_ALL/storeday=2018-01-01")


Catalog	        hive_metastore
Created Time	Tue May 30 02:16:44 UTC 2023
Last Access	UNKNOWN
Created By	Spark 3.1.2
Type	        EXTERNAL
Location	wasbs://landing-zone@stprdnaeus2.blob.core.windows.net/on-premises/oracle-
                    r12/PROD/Contraloria/PO/RCV_TRANSACTIONS/storeday=2019-07-01
Serde Library	org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
Input Format	org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
Output Format	org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "11", "key" : [ "UDF", "RESUABLE", "COMPONANTS" ] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

udf -> custom logic -> resuable componants
          create function inside spark sql & spark Python
 
UDF’s are the most expensive operations hence use them only you have no choice and when essential.

++++++++++++++++++++++++++++
- How to register
++++++++++++++++++++++++++++

spark.udf.register("concat_cols",concat)
OR
concat_cols = udf(concat, StringType())

++++++++++++++++++++++++++++
- Simple udf : uppercase  
++++++++++++++++++++++++++++

file_01 = "abfss://raw@sttpldatasolutionsdev.dfs.core.windows.net/sos/temp_files/csv_files/data_01.csv"
file_02 = "abfss://raw@sttpldatasolutionsdev.dfs.core.windows.net/sos/temp_files/csv_files/data_02.csv"

df = spark.read.options(header='True') \
    .options(delimiter=';') \
    .csv(path=[ file_01, file_02 ] )
    
#####

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


def upperCase(str):
    return str.upper()

#####

upperCaseUDF = udf(lambda z:upperCase(z))
- OR - 
upperCaseUDF = udf(lambda z:upperCase(z),StringType())  

#####

df_query =   df.select(upperCaseUDF(col("emp_name")).alias("Name"), col("emp_id"))
display(df_query)


++++++++++++++++++++++++++++
- Simple udf : square root
++++++++++++++++++++++++++++

file_01 = "abfss://raw@sttpldatasolutionsdev.dfs.core.windows.net/sos/temp_files/csv_files/data_01.csv"
file_02 = "abfss://raw@sttpldatasolutionsdev.dfs.core.windows.net/sos/temp_files/csv_files/data_02.csv"

df = spark.read.options(header='True') \
    .options(delimiter=';') \
    .csv(path=[ file_01, file_02 ] )
    
#####

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

squareUDF = udf(lambda z : int(z) * 2,IntegerType())

#####

df_query =   df.select(squareUDF(col("emp_id")).alias("id"), col("emp_name"))
display(df_query)

++++++++++++++++++++++++++++
- Simple udf : square root
++++++++++++++++++++++++++++

from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com') \
                    .getOrCreate()

# Prepare data
data=data = [('James','','Smith','1991-04-01'),
  ('Michael','Rose','','2000-05-19'),
  ('Robert','','Williams','1978-09-05'),
  ('Maria','Anne','Jones','1967-12-01'),
  ('Jen','Mary','Brown','1980-02-17')
]

columns=["firstname","middlename","lastname","dob"]
df=spark.createDataFrame(data,columns)
df.printSchema()
df.show(truncate=False)

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# udf function
def concat(x, y, z):
    return x +' '+ y + ' ' + z

concat_cols = udf(concat, StringType())

# using udf
df9 = df.withColumn("Full_Name",concat_cols(df.firstname,df.middlename, df.lastname)) 
display(df9)

++++++++++++++++++++++++++++
REGISTRY FOR SQL
++++++++++++++++++++++++++++

### spark.udf.register("convertUDF", convertCase,StringType())

spark.udf.register("UDF_sql_ucase", upperCaseUDF)

df.createOrReplaceTempView("temp_table")

df2 = spark.sql("select Seqno, UDF_sql_ucase(Name) as Name from table") 
df2.show(truncate=False)



++++++++++++++++++++++++++++
UN - REGISTRY FOR SQL
++++++++++++++++++++++++++++

spark.sql("drop temporary function UDF_sql_ucase")


++++++++++++++++++++++++++++
FUNCTION LIST
++++++++++++++++++++++++++++

%sql
SHOW USER FUNCTIONS;

#####

%sql
CREATE FUNCTION sandbox.sos.convert_f_to_c(unit STRING, temp DOUBLE)
RETURNS DOUBLE
RETURN CASE
  WHEN unit = "F" THEN (temp - 32) * (5/9)
  ELSE temp
END;

#####

%sql

CREATE FUNCTION sandbox.sos.hello() RETURNS STRING RETURN 'Hello World!';

CREATE TEMPORARY FUNCTION hello2() RETURNS STRING RETURN 'Good morning!';

#####

%sql
DROP FUNCTION sandbox.sos.convert_f_to_c;
DROP TEMPORARY FUNCTION IF EXISTS hello;

#####

%sql
DESCRIBE FUNCTION extended hello2;

DESCRIBE FUNCTION hello2;

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "12", "key" : [ "XLS", "XLSX", "READ_EXCEL" ] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

import pandas as pd

file_name= "zip_codes.xlsx"
sheet_name = "zip_codes"

xls = pd.read_excel(file_name, sheet_name=['zip1', 'zip2'] )

# Access individual sheets using sheet names
df = xls["zip1"]
df2 = xls["zip2"]

for index, row in df.iterrows():
    print(index, row["Country"], row["City"])
    


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "13", "key" : [ "LIMIT", "ROWNUM" ] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

df.show(10)

df1 = df.take(10)

df1 = df.limit(10)

df1 = df.tail(10)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "14", "key" : [ "MISMATCH", "SUBSTRACT"] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
from pyspark.sql.functions import *

data1 =[["1", "sravan", "tcs"],
       ["3", "bobby", "hdfc"   ],
       ["2", "ojaswi", "wipro"],
       ["1", "sravan", "hcl"   ],
       ["3", "bobby", "dell"  ],
       ["1", "rohith", "tcs"   ],
       ["1", "sravan", "tcs"]]
  
columns = ['employee_id','employee_name','company_name']
df1 = spark.createDataFrame(data1,columns)
df11 = df1.select("company_name").distinct()
display(df11)

data2 =[["1", "sravan", "nic"],
       ["3", "bobby", "hdfc"   ],
       ["2", "ojaswi", "wipro"],
       ["1", "sravan", "hcl"   ],
       ["3", "bobby", "dell"  ],
       ["1", "rohith", "tcs"   ],
       ["1", "sravan", "Lotus"]]
  
columns = ['employee_id','employee_name','company_name']
df2 = spark.createDataFrame(data2,columns)
df22 = df2.select("company_name").distinct()
display(df22)

distinct_company = df22.subtract(df11).collect()
print(distinct_company)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "15", "key" : [ "NULL", "NONE" ] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

++++++++++++++++++++++++++++
- Find Null Values
++++++++++++++++++++++++++++

simpleData = (("Java",4000,5),
    ("Python", 4600,10),
    ("Scala",None,15),
    ("Scala", None,15),
    ("PHP", 3000,20),
  )
columns= ["CourseName", "fee", "discount"]

# Create DataFrame
df = spark.createDataFrame(data = simpleData, schema = columns)
df.show(truncate=False)

df1 = df.filter("fee is NULL")
display(df1)

#####

df1 = df.filter("fee is NOT NULL")

#####

df1 = df.filter("NOT fee is NULL")

#####

df2 = df.filter("fee is NULL AND discount is NULL")
display(df2)

#####

df = spark.sql("SELECT * FROM DATA where STATE IS NULL")
display(df)

#####

%sql
select  *  
from sandbox.sos.results
where ResultFormattedEntry IS NULL

++++++++++++++++++++++++++++
- Transformation Null Value
++++++++++++++++++++++++++++

from pyspark.sql.functions import *
from pyspark.sql import *

data = [ (1,'<1'),  (2,'<0'),  (3,'3'), (4, '-')]
schema = [ 'id','value' ]
df = spark.createDataFrame(data = data,schema=schema)
df.show()

data = [ ('<1', 0),  ('<0', 0),   ('-', 9)]
schema = [ 'trf_id','trf_value' ]
trf = spark.createDataFrame(data = data,schema=schema)
trf.show()

df_join = df.join(trf, df.value == trf.trf_id, "left")

display(df_join)

df2 = df_join.select(
        col("id") ,
        col("value") ,
        when( df_join.trf_value.isNull(), df_join.id
        ).otherwise(df_join.trf_value).alias("xxxx")  
    )  

display(df2)

#####

df2 = df_join.select(
        col("id") ,
        col("value") ,
        when( col("trf_value").isNull(), col("id")).otherwise(col("trf_value")).alias("UpdatedValue")
    ) 
	
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "16", "key" : [ "IF", "THEN", "ELSE" ] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

++++++++++++++++++++++++++++
- IF ELSE : Single 
++++++++++++++++++++++++++++

.select(
    col('sampleNum').alias('sampleNum'),
    when( ( col("ResultFormattedEntry") == "<1" ) | (col("ResultFormattedEntry") == "<0" ), "0").otherwise
   .alias('ResultFormattedNumeric')
)


++++++++++++++++++++++++++++
- IF ELSE : Multilevel 
++++++++++++++++++++++++++++

.select(
        col('sampleNum').alias('sampleNum'),
        when( ( col("ResultFormattedEntry") == "<1" ) | (col("ResultFormattedEntry") == "<0" ), "0").otherwise
        (
            when(
                col("ResultFormattedEntry").cast("int").isNotNull().alias("Value") == False, "NULL")
                .otherwise(col("ResultFormattedEntry")
            )
            .alias('ResultFormattedNumeric')
        .alias('ResultFormattedEntry')
        )
        .alias('ResultFormattedNumeric')
)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "17", "key" : [ "GROUP BY", "GROUP" ] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
- Group by 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

count()
min()
max()
avg()
mean()

#####

df.groupBy("department").count()           .show()
df.groupBy("department").min("salary")   .show()
df.groupBy("department").max("salary")   .show()
df.groupBy("department").avg("salary")    .show()
df.groupBy("department").mean("salary") .show()

#####

df.groupBy("state").agg(sum("salary")).show()


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
- Group by using describe df.describe
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

simpleData = (("Java",4000,5), 
    ("Python", 4600,10),  
    ("Scala", 4100,15),   
    ("Scala", 4500,15),   
    ("PHP", 3000,20),  
  )

columns= ["CourseName", "fee", "discount"]

# Create DataFrame
df = spark.createDataFrame(data = simpleData, schema = columns)

df1 = df.describe("fee")
display(df1)

zz = float(df.describe("fee").filter("summary = 'max'").select("fee").first().asDict()['fee'])

print(zz)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
- Group by data from database delta table
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++


simpleData = [
    ( "James","Sales","NY",90000,34,10000      ),
    ( "Michael","Sales","NV",86000,56,20000    ),
    ( "Robert","Sales","CA",81000,30,23000     ),
    ( "Maria","Finance","CA",90000,24,23000    ),
    ( "Raman","Finance","DE",99000,40,24000  ),
    ( "Scott","Finance","NY",83000,36,19000    ),
    ( "Jen","Finance","NY",79000,53,15000       ),
    ( "Jeff","Marketing","NV",80000,25,18000    ),
    ( "Kumar","Marketing","NJ",91000,50,21000 )
]
schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)

df.groupBy("state").sum("salary").withColumnRenamed("sum(salary)", "sumSalary").show()


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
- SQL - Group by 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

df.createOrReplaceTempView("EMP")
spark.sql("select state, sum(salary) as sum_salary from EMP group by state").show()


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
- group on dataframe
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

simpleData = (("Java",4000,5), 
    ("Python", 4600,10),  
    ("Scala", 4100,15),   
    ("Scala", 4500,15),   
    ("PHP", 3000,20),  
  )

columns= ["CourseName", "fee", "discount"]

# Create DataFrame
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

# Using max() function
from pyspark.sql.functions import max
df.select(max(df.fee)).show()

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "18", "key" : [ "ORDER", "ORDER BY" ] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++++++++++++++++++++++++++++
- Order By
++++++++++++++++++++++++++++

import pyspark 
  
data = [
        ["1", "sravan", "company 3"], 
        ["2", "ojaswi", "company 4"], 
        ["3", "rohith", "company 2"], 
        ["4", "sridevi", "company 1"], 
        ["5", "bobby", "company 1"]
] 
  
columns = ['ID', 'NAME', 'Company'] 
dataframe = spark.createDataFrame(data, columns) 
  
dataframe.orderBy([ 'ID', 'Company'], ascending=True).show() 

#####

dataframe.orderBy([ 'ID', 'Company'], ascending=False).show() 

#####

df_csv.groupBy("dept")
    .count()
    .orderBy(asc("count"))
    .show()

#####

import pyspark 
  
data = [["1", "sravan", "company 1"], 
        ["2", "ojaswi", "company 1"], 
        ["3", "rohith", "company 2"], 
        ["4", "sridevi", "company 1"], 
        ["5", "bobby", "company 1"]] 
  
columns = ['ID', 'NAME', 'Company'] 
dataframe = spark.createDataFrame(data, columns) 
  
dataframe.orderBy([ 'ID', 'Company'], ascending=True).show() 

#####

dataframe.orderBy([ 'ID', 'Company'], ascending=False).show() 

#####

dataframe.sort(['Name', 'ID', 'Company'], ascending=True).show()

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "19", "key" : [ "ROWNUM", "THEN" ] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++++++++++++++++++++++++++++
- Row Number column as Index
++++++++++++++++++++++++++++

def ret_value():
    id = 10
    return id


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

data =[["1", "sravan", "tcs"],
       ["3", "bobby", "hdfc"   ],
       ["2", "ojaswi", "wipro"],
       ["1", "sravan", "hcl"   ],
       ["3", "bobby", "dell"  ],
       ["1", "rohith", "tcs"   ],
       ["1", "sravan", "tcs"]]
  
columns = ['employee_id','employee_name','company_name']
df = spark.createDataFrame(data,columns)
display(df)

df_update = df.withColumn("seq_num", row_number().over(Window.orderBy("company_name")))

# df_update.show()

df9 = (
        df_update
        .withColumn('seq_num_new', col('seq_num')) # + ret_value())
)

display(df9)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "20", "key" : [ "TYPE", "CONVERSION", "TYPE CONVERSION" ] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++++++++++++++++++++++++++++
- String to Integer
++++++++++++++++++++++++++++
.select( col('sampleNum')  .alias('sampleNum').cast('int')


++++++++++++++++++++++++++++
- String to Float / Double
++++++++++++++++++++++++++++
.select( col('sampleNum')  .alias('sampleNum').cast('double')


++++++++++++++++++++++++++++
- String to Time Stamp
++++++++++++++++++++++++++++
.withColumn('ResultChangedOn',  to_timestamp(col("ArrayResult.resultChangedOn")))

#####

df.withColumn("salary",round(df.salary.cast(DoubleType()),2))

#####

from pyspark.sql.types import DoubleType 
df = df.withColumn("age", df["age"].cast(DoubleType())) 

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "21", "key" : [ "DLT", "EXCEPTION", "EXCEPT" ] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

++++++++++++++++++++++++++++
- Single Exception
++++++++++++++++++++++++++++

### Retain invalid records
@dlt.expect("valid timestamp", "col(“timestamp”) > '2012-01-01'")


### Drop invalid records
@dlt.expect_or_drop("valid_current_page", "current_page_id IS NOT NULL AND current_page_title IS NOT NULL")


### Fail on invalid records
@dlt.expect_or_fail("valid_count", "count > 0")


++++++++++++++++++++++++++++
- Multiple Exception
++++++++++++++++++++++++++++

### Retain invalid records
@dlt.expect_all({"valid_count": "count > 0", "valid_current_page": "current_page_id IS NOT NULL AND current_page_title IS NOT NULL"})


### Drop invalid records
@dlt.expect_all_or_drop({"valid_count": "count > 0", "valid_current_page": "current_page_id IS NOT NULL AND current_page_title IS NOT NULL"})


### Fail on invalid records
@dlt.expect_all_or_fail({"valid_count": "count > 0", "valid_current_page": "current_page_id IS NOT NULL AND current_page_title IS NOT NULL"})


++++++++++++++++++++++++++++
- Exception in a variable
++++++++++++++++++++++++++++

valid_pages = {"valid_count": "count > 0", "valid_current_page": "current_page_id IS NOT NULL AND current_page_title IS NOT NULL"}

@dlt.table
@dlt.expect_all(valid_pages)
def raw_data():
    return retdf


++++++++++++++++++++++++++++
- Exception as an array / dictionary
++++++++++++++++++++++++++++

import dlt
from pyspark.sql.functions import expr

rules = {}
rules["valid_website"] = "(Website IS NOT NULL)"
rules["valid_location"] = "(Location IS NOT NULL)"
quarantine_rules = "NOT({0})".format(" AND ".join(rules.values()))

@dlt.table(
  name="raw_farmers_market"
)
def get_farmers_market_data():
  return (
    spark.read.format('csv').option("header", "true")
      .load('/databricks-datasets/data.gov/farmers_markets_geographic_data/data-001/')
  )

@dlt.table(
  name="farmers_market_quarantine",
  temporary=True,
  partition_cols=["is_quarantined"]
)
@dlt.expect_all(rules)
def farmers_market_quarantine():
  return (
    dlt.read("raw_farmers_market")
      .select("MarketName", "Website", "Location", "State",
              "Facebook", "Twitter", "Youtube", "Organic", "updateTime")
      .withColumn("is_quarantined", expr(quarantine_rules))
  )

@dlt.view(
  name="valid_farmers_market"
)
def get_valid_farmers_market():
  return (
    dlt.read("farmers_market_quarantine")
      .filter("is_quarantined=false")
  )

@dlt.view(
  name="invalid_farmers_market"
)
def get_invalid_farmers_market():
  return (
    dlt.read("farmers_market_quarantine")
      .filter("is_quarantined=true")
  )

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "22", "key" : [ "JOIN" ] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

++++++++++++++++++++++++++++
- Joins in dataframe
++++++++++++++++++++++++++++

01. inner

02. left
     leftouter
     left_outer

03. right
     rightouter
     right_outrer

Join either matched OR not matched both the side with 3 different ways
04. outer
      full
      fullouter
      full_outer

05. leftsemi

06. leftanti

07. Cross Join
08. Self Join

++++++++++++++++++++++++++++
- Joins  - inner
++++++++++++++++++++++++++++

from pyspark.sql import SparkSession

left = spark.createDataFrame([("Chandu", 1), ("Manish", 2), ("Manju", 3)], ["fname", "fcity"])

right = spark.createDataFrame([(1, "lucknow"), (4, "Jammu")], ["city_id", "city_name"])

inner_join = left.join(right, left.fcity == right.city_id, "inner").show()

##### 

# JOIN emp and dept
empDF.join(deptDF).where(empDF["emp_dept_id"] == deptDF["dept_id"]).show()

##### 

# JOIN IN 3 TABLE
empDF.join(deptDF).where(empDF["emp_dept_id"] == deptDF["dept_id"]) \
    .join(addDF).where(empDF["emp_id"] == addDF["emp_id"]) \
    .show()
  
++++++++++++++++++++++++++++
- Columns name are same in Joins 
++++++++++++++++++++++++++++

Ex:
data1 = (
  (1, "Product A", 100),
  (2, "Product B", 200),
  (3, "Product C", 150)
)
df1 = spark.createDataFrame(data1).toDF("id", "product", "quantity")

data2 = (
  (1, "Product A", 1000),
  (2, "Product B", 1500),
  (3, "Product C", 1200)
)
df2 = spark.createDataFrame(data2).toDF("id", "product", "revenue")

joinedDF1 = df1.join(df2, "id")

#####

df1.join(df2, on='id', how='inner').show()

#####

++++++++++++++++++++++++++++
- Join with Null Column
++++++++++++++++++++++++++++

 .join(compartments, samples.Compartment == compartments.Description, "inner")

 .join(compartments, samples.Compartment.eqNullSafe( compartments.Description ), "inner") 

++++++++++++++++++++++++++++
- Databricks SQL Join
++++++++++++++++++++++++++++

empDF.createOrReplaceTempView("emp")
deptDF.createOrReplaceTempView("dept")
addDF.createOrReplaceTempView("add")

df_sql = spark.sql("select * from emp e, dept d, add a " + \
            "where e.emp_dept_id == d.dept_id and e.emp_id == a.emp_id") \

display(df_sql)
    
#####

df.queryExecution.executionPlan

---> from comand prompt

%sql
select * from 
    sandbox.sos.merged_tests t,
    sandbox.sos.units m
where t.ResultName = m.ResultName
   and t.ResultUnits  = m.ResultUnits

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "23", "key" : [ "DBUTILS", "DBUTIL" ] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 DBUTILS
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Parameter :
   1. path = complete path of notebook
   2. timeout = 0 stand for no timeout limit
   3. other paramert as a dictionary

dbutils.notebook.run(path,0,{"CorrelationId": correlationId})

---> PYTHON 

import os
os.listdir("/dbfs/tmp")

---> DBUTILS

dbutils.fs.ls("fileStore/table")

dbutils.fs.ls("dbfs:/user/hive/warehouse/customer/state=UP/city=LKO")

#####

dbutils.fs.head("fileStore/table/a.csv")

#####

dbutils.fs.mkdirs("/FileStore/data")

dbutils.fs.ls("/FileStore")
Output

[FileInfo(path='dbfs:/FileStore/data/', name='data/', size=0, modificationTime=0),
 FileInfo(path='dbfs:/FileStore/tables/', name='tables/', size=0, modificationTime=0)]


#####

dbutils.fs.cp("fileStore/table/a.csv", "fileStore/table/new_folder")

#####

dbutils.fs.mv("fileStore/table/a.csv", "fileStore/table/new_folder")

#####

dbutils.fs.put("fileStore/table/new_file.csv", "This is the contents of the file")

---> delete File
dbutils.fs.rm("fileStore/table/a.csv")

---> delete Folder
dbutils.fs.rm("fileStore/table/", True)

---> RUN NOTEBOOK

dbutils.notebook.run(path,60)  # 60 IS THE TIMEOUT IN SECONDS

%run /etl/process/canada_ic/validation  $folder_name="folder_name" $file_name="dile_name.csv"

#####

%run 
  is copying code from another notebook and executing it within the one its called from. 
  All variables defined in the notebook being called are therefore visible to the caller notebook

dbutils.notebook.run() 
  is more around executing different notebooks in a workflow, an orchestration 
  of sorts. Each notebook runs in an isolated spark session and passing parameters and return values 
  is through a strictly defined interface

---> SET THE WIDGETS

dbutils.widgets.text(input_name, input_value, default_value)
dbutils.widgets.text("process_name","Canada_IC")

dbutils.widgets.dropdown("drop_down","1",[ str(x) for x in range(1,10) ])

dbutils.widgets.combobox("drop_down","1",[ str(x) for x in range(1,10) ])

dbutils.widgets.multiselect("multiselect","Camera", ( "mobile","internet","landline" ))

---> GET THE WIDGETS

process_name = dbutils.widgets.get("process_name")

#####

dbutils.widgets.remove("process_name")
dbutils.widgets.removeAll()

#####

display(dbutils.fs.ls("/FileStore/tables/stream_csv/checkpoint/"))

display(dbutils.fs.ls("dbfs:/tmp/test.json")
display(dbutils.fs.ls("/tmp/test.json")

write data into file
dbutils.fs.put("a.csv","file contents")

dbutils.fs.head("/a.csv") - display contents of a file

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "24", "key" : [ "DLT", "DB", "CONNECTION", "DLT.DB.CONNECTION" ] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
- DLT table from sql server database table
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

import dlt

@dlt.table
def postgres_raw():
  return (
    spark.read
      .format("postgresql")
      .option("dbtable", table_name)
      .option("host", database_host_url)
      .option("port", 5432)
      .option("database", database_name)
      .option("user", username)
      .option("password", password)
      .load()
  )
  
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "25", "key" : [ "FILTER" ] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

file = 'dbfs:/FileStore/tables/AUTOMATION_process_master.csv'

df = spark.read.csv(path=file, header=True)
display(df)

##### 

df1 = df.filter(df.team_id == "3")
display(df1)

---> AND

df2 = df.filter((df.team_id == "3") & (df.process_frequency == "D"))
display(df2)

---> OR

df2 = df.filter((df.team_id == "3") | (df.process_frequency == "D"))
display(df2)

---> USING COLUMN FUNCTION

from pyspark.sql.functions import col

df2 = df.filter(col("team_id") == "3")
display(df2)

#####

dataframe.filter( (col("team_id ") == "3") & (col("process_frequency") == "D") )

---> IN 

df2 = df.filter(df.process_frequency.isin(["D","R"]))
display(df2)

#####

college_list = ['DU','IIT']
df2 = df.filter( (df.student_ID.isin(Id_list)) | (df.college.isin(college_list)) )

---> START WITH ,  END WIDTH

df2 = df.filter(df.process_name.startswith('A'))
df2 = df.filter(df.process_name.endswith('g'))

df2 = df.filter( (df.process_name.endswith('t')) & (df.process_name.startswith("A")) )

#####

++++++++++++++++++++++++++++
DOUBT-FULL
++++++++++++++++++++++++++++

df.where(df("state") === "OH").show(false)
df.where('state === "OH").show(false)
df.where($state === "OH").show(false)
df.where(col("state") === "OH").show(false)
3. DataFrame filter() with SQL Expression
If you are coming from SQL background, you can use that knowledge in Spark to filter DataFrame 
rows with SQL expressions.


df.filter("gender == 'M'").show(false)
df.where("gender == 'M'").show(false)
This yields below DataFrame results.


+----------------------+------------------+-----+------+
|name                  |languages         |state|gender|
+----------------------+------------------+-----+------+
|[James, , Smith]      |[Java, Scala, C++]|OH   |M     |
|[Maria, Anne, Jones]  |[CSharp, VB]      |NY   |M     |
|[Jen, Mary, Brown]    |[CSharp, VB]      |NY   |M     |
|[Mike, Mary, Williams]|[Python, VB]      |OH   |M     |
+----------------------+------------------+-----+------+
4. Filter with Multiple Conditions
To filter() rows on Spark DataFrame based on multiple conditions using AND(&&), OR(||), 
and NOT(!), you case use either Column with a condition or SQL expression as explained 
above. Below is just a simple example, you can extend this with AND(&&), OR(||), and NOT(!) 
conditional expressions as needed.


//multiple condition
df.filter(df("state") === "OH" && df("gender") === "M")
    .show(false)
This yields below DataFrame results.


+----------------------+------------------+-----+------+
|name                  |languages         |state|gender|
+----------------------+------------------+-----+------+
|[James, , Smith]      |[Java, Scala, C++]|OH   |M     |
|[Mike, Mary, Williams]|[Python, VB]      |OH   |M     |
+----------------------+------------------+-----+------+
5. Filter on an Array Column
When you want to filter rows from DataFrame based on value present in an array collection column, 
you can use the first syntax. The below example uses array_contains() Spark SQL function which 
checks if a value contains in an array if present it returns true otherwise false.


import org.apache.spark.sql.functions.array_contains
df.filter(array_contains(df("languages"),"Java"))
    .show(false)
This yields below DataFrame results.


+----------------+------------------+-----+------+
|name            |languages         |state|gender|
+----------------+------------------+-----+------+
|[James, , Smith]|[Java, Scala, C++]|OH   |M     |
|[Anna, Rose, ]  |[Spark, Java, C++]|NY   |F     |
+----------------+------------------+-----+------+
6. Filter on Nested Struct columns
If your DataFrame consists of nested struct columns, you can use any of the above syntaxes 
to filter the rows based on the nested column.


  //Struct condition
df.filter(df("name.lastname") === "Williams")
    .show(false)
This yields below DataFrame results


+----------------------+------------+-----+------+
|name                  |languages   |state|gender|
+----------------------+------------+-----+------+
|[Julia, , Williams]   |[CSharp, VB]|OH   |F     |
|[Mike, Mary, Williams]|[Python, VB]|OH   |M     |
+----------------------+------------+-----+------+

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
Source code of Spark DataFrame Where Filter
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.functions.array_contains

object FilterExample extends App{
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val arrayStructureData = Seq(
    Row(Row("James","","Smith"),List("Java","Scala","C++"),"OH","M"),
    Row(Row("Anna","Rose",""),List("Spark","Java","C++"),"NY","F"),
    Row(Row("Julia","","Williams"),List("CSharp","VB"),"OH","F"),
    Row(Row("Maria","Anne","Jones"),List("CSharp","VB"),"NY","M"),
    Row(Row("Jen","Mary","Brown"),List("CSharp","VB"),"NY","M"),
    Row(Row("Mike","Mary","Williams"),List("Python","VB"),"OH","M")
  )

  val arrayStructureSchema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("languages", ArrayType(StringType))
    .add("state", StringType)
    .add("gender", StringType)

  val df = spark.createDataFrame(
   spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
  df.printSchema()
  df.show()

  //Condition
  df.filter(df("state") === "OH")
    .show(false)

  //SQL Expression
  df.filter("gender == 'M'")
    .show(false)

  //multiple condition
  df.filter(df("state") === "OH" && df("gender") === "M")
    .show(false)

  //Array condition
  df.filter(array_contains(df("languages"),"Java"))
    .show(false)

  //Struct condition
  df.filter(df("name.lastname") === "Williams")
    .show(false)
}

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "26", "key" : [ "DISTINCT", "UNIQUE" ] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
- Distinct Data
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

import pyspark
  
from pyspark.sql import SparkSession
  
data =[["1", "sravan", "tcs"],
       ["3", "bobby", "hdfc"   ],
       ["2", "ojaswi", "wipro"],
       ["1", "sravan", "hcl"   ],
       ["3", "bobby", "dell"  ],
       ["1", "rohith", "tcs"   ],
       ["1", "sravan", "tcs"]]
  
columns = ['employee_id','employee_name','company_name']
df = spark.createDataFrame(data,columns)
display(df)

#####

# Select distinct rows
distinctDF = df.distinct()
distinctDF.show(truncate=False)

#####

### Single column distinct value
df.select("company_name").distinct().show()

#####

### Multi columns distinct value
df.select(["employee_id","company_name"]).distinct().show()

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
~{ "index" : "27", "key" : [ "FILTER" ] }
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> TOPIC
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

orderBy()
groupBy()
filter()
withColumn()
withColumns()

Setup and Basics
SparkSession, DataFrame creation, and basic operations.

DataFrame Operations
Selecting, filtering, grouping, joining, sorting, and aggregating.

SQL Expressions
Using expr() and selectExpr() for SQL-like transformations.

Column Transformations
Using withColumn() and withColumns() for adding/modifying columns.

Joins and Combining Data
Types of joins (inner, left, right, outer, etc.).

Aggregations and Grouping
GroupBy, aggregations (sum, count, avg, etc.), and window functions.

Handling Nulls and Missing Data
Managing null values with COALESCE, na.drop(), etc.

Date and Time Operations
Date manipulations using SQL functions.

Complex Data Types
Arrays, structs, and maps.

User-Defined Functions (UDFs)
Custom Python functions for DataFrame operations.

Performance Optimization
Caching, partitioning, and broadcast joins.

Spark SQL
Running SQL queries directly on DataFrames.
Input/Output Operations
Reading/writing data (CSV, JSON, Parquet, etc.).

Streaming
Structured Streaming for real-time data processing.

Machine Learning with MLlib
Basic ML pipelines and transformations.



+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> WHEN WITH SELECT
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql.functions import col, when

df = df.select(
    col("id").alias("user_id"),
    col("name").alias("full_name"),
    col("status").alias("account_status"),
    when((col("record").isNull()) | (col("record") == ""), "ER")
        .otherwise(col("record"))
        .alias("record_cleaned")
)

--------------------------------------
>> 
--------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

jdbc_url = "jdbc:postgresql://host:port/db"
conn_props = {
    "user": "your_user",
    "password": "your_pass",
    "driver": "org.postgresql.Driver"
}

# Get raw Java connection
conn = spark._sc._jvm.java.sql.DriverManager.getConnection(jdbc_url, conn_props["user"], conn_props["password"])
conn.setAutoCommit(False)

try:
    stmt = conn.createStatement()
    
    # Use SQL insert manually — or call stored procedure
    stmt.executeUpdate("INSERT INTO table1 (col1, col2) VALUES ('a', 'b')")
    stmt.executeUpdate("INSERT INTO table2 (col1, col2) VALUES ('x', 'y')")
    
    conn.commit()
    print("Transaction successful")
except Exception as e:
    conn.rollback()
    print(f"Transaction rolled back due to error: {e}")
finally:
    conn.close()


--------------------------------------
>> 
--------------------------------------

if condition1:
    if condition1a:
        result = value1a
    elif condition1b:
        result = value1b
    else:
        result = value1_default
else:
    result = value_outside

withColumn(
  "status",
  when(col("score") > 90, "A")
  .when(col("score") > 80, "B")
  .when(col("score") > 70, "C")
  .otherwise("F")
)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> EMPTY, LINE, BLANK, LAMBDA 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql.functions import col

df.select(
    col("your_column").cast("date").alias("your_date_column")
)

---

non_empty_condition = ~(
    reduce(
        lambda a, b: a & b,
        [(col(c).isNull() | (col(c) == "")) for c in df.columns]
    )
)

df_cleaned = df.filter(non_empty_condition)

###

df_cleaned = df9.filter(
    ~(
        (col("product_type").isNull()      | ( trim(col("product_type")) == ""))   &
        (col("reserve_ag_dlr").isNull()    | ( trim(col("reserve_ag_dlr")) == "")) &
        (col("reserve_date").isNull()      | ( trim(col("reserve_date")) == "")) 
    )
)

---

df9 = df_IN_raw.select(
    when((col("product_type").isNull()) | (trim(col("product_type")) == ""), lit(" ")).otherwise(col("product_type")).alias("product_type"),
    when((col("defect_code").isNull())  | (trim(col("defect_code")) == ""), lit(" ")).otherwise(col("defect_code")).alias("defect_code"),
)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>>  
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql.functions import col, when, lit

# Sample df1
df1 = spark.createDataFrame([
    (1, "Rahul"),
    (2, "Amit"),
    (3, "Priya"),
    (4, "Sneha")
], ["ID", "Name"])

# Sample df2 with duplicate IDs
df2 = spark.createDataFrame([
    (1, 85),
    (3, 90),
    (3, 95),
    (5, 70)
], ["ID", "Marks"])

# Get distinct IDs from df2
df2_unique_ids = df2.select("ID").distinct()

# Left join df1 with distinct df2 IDs
df_joined = df1.join(df2_unique_ids.withColumnRenamed("ID", "df2_ID"), df1.ID == col("df2_ID"), "left")

# Create 'Exists_in_df2' column
df_result = df_joined.withColumn(
    "Exists_in_df2",
    when(col("df2_ID").isNotNull(), lit("Yes")).otherwise(lit("No"))
)

# Select final columns
df_result.select("ID", "Name", "Exists_in_df2").show()

+++ Note
df1.ID works fine — it’s like a shortcut.

col("df2_ID") is used when the column is not directly referenced from a DataFrame object, or it's renamed.

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>>  
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql.functions import col

# Perform inner join on id
joined_df = input_df.alias("in").join(
    output_df.alias("out"),
    on="id",
    how="left"
)

# Filter only rows where at least one column doesn't match
filtered_df = joined_df.filter(
    (col("in.name")    != col("out.name"))  |
    (col("in.age")     != col("out.age"))   |
    (col("in.salary")  != col("out.salary"))|
    (col("in.add")     != col("out.add"))   |
    col("out.id").isNull()                 # Also keep rows where ID was not found in output_df
).select("in.*")  # Keep only columns from input_df

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> EXPR, EXPR()
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Key Notes
Performance: expr() is optimized as it’s executed within the Catalyst optimizer, similar to native PySpark functions.
SQL Functions: You can use any SQL function supported by Spark SQL (e.g., UPPER, LOWER, DATEDIFF, SUM, etc.).
Limitations: Complex logic may be better handled with native PySpark functions or UDFs for readability and maintainability.
Debugging: Ensure the SQL expression syntax is correct, as errors in expr() can sometimes be cryptic.

When to Use expr()?
When you need SQL-like syntax for quick prototyping.
For dynamic expressions based on runtime conditions.

---

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Initialize Spark session
spark = SparkSession.builder.appName("ExprExamples").getOrCreate()

# Sample data
data = [
    ("Alice", 25, 50000, "2023-01-15", "F"),
    ("Bob", 30, 60000, "2022-06-20", "M"),
    ("Cathy", 28, 75000, "2021-09-10", "F"),
    ("David", None, 45000, "2020-03-05", "M")
]
columns = ["name", "age", "salary", "join_date", "gender"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
df.show()
+-----+----+------+----------+------+
| name| age|salary| join_date|gender|
+-----+----+------+----------+------+
|Alice|  25| 50000|2023-01-15|     F|
|  Bob|  30| 60000|2022-06-20|     M|
|Cathy|  28| 75000|2021-09-10|     F|
|David|null| 45000|2020-03-05|     M|
+-----+----+------+----------+------+

1. Arithmetic Operations
# Increase salary by 10% and create a new column
df_arithmetic = df.withColumn("salary_increased", expr("salary * 1.10"))
df_arithmetic.show()
+-----+----+------+----------+------+---------------+
| name| age|salary| join_date|gender|salary_increased|
+-----+----+------+----------+------+---------------+
|Alice|  25| 50000|2023-01-15|     F|        55000.0|
|  Bob|  30| 60000|2022-06-20|     M|        66000.0|
|Cathy|  28| 75000|2021-09-10|     F|        82500.0|
|David|null| 45000|2020-03-05|     M|        49500.0|
+-----+----+------+----------+------+---------------+

2. String Manipulation
# Concatenate name and gender with a separator
df_string = df.withColumn("name_gender", expr("CONCAT(name, ' - ', gender)"))
df_string.show()
+-----+----+------+----------+------+-----------+
| name| age|salary| join_date|gender|name_gender|
+-----+----+------+----------+------+-----------+
|Alice|  25| 50000|2023-01-15|     F|  Alice - F|
|  Bob|  30| 60000|2022-06-20|     M|    Bob - M|
|Cathy|  28| 75000|2021-09-10|     F|  Cathy - F|
|David|null| 45000|2020-03-05|     M|  David - M|
+-----+----+------+----------+------+-----------+

3. Conditional Logic (CASE WHEN)
# Categorize salary into Low, Medium, High
df_conditional = df.withColumn(
    "salary_category",
    expr("""
        CASE 
            WHEN salary < 50000 THEN 'Low'
            WHEN salary BETWEEN 50000 AND 70000 THEN 'Medium'
            ELSE 'High'
        END
    """)
)
df_conditional.show()
+-----+----+------+----------+------+---------------+
| name| age|salary| join_date|gender|salary_category|
+-----+----+------+----------+------+---------------+
|Alice|  25| 50000|2023-01-15|     F|         Medium|
|  Bob|  30| 60000|2022-06-20|     M|         Medium|
|Cathy|  28| 75000|2021-09-10|     F|           High|
|David|null| 45000|2020-03-05|     M|            Low|
+-----+----+------+----------+------+---------------+

4. Handling Null Values
# Replace null age with average age (approx 27.67)
df_null_handling = df.withColumn("age_filled", expr("COALESCE(age, 27)"))
df_null_handling.show()
+-----+----+------+----------+------+----------+
| name| age|salary| join_date|gender|age_filled|
+-----+----+------+----------+------+----------+
|Alice|  25| 50000|2023-01-15|     F|        25|
|  Bob|  30| 60000|2022-06-20|     M|        30|
|Cathy|  28| 75000|2021-09-10|     F|        28|
|David|null| 45000|2020-03-05|     M|        27|
+-----+----+------+----------+------+----------+

5. Date and Time Operations
# Add 30 days to join_date
df_date = df.withColumn("join_date_plus_30", expr("DATE_ADD(join_date, 30)"))
df_date.show()
+-----+----+------+----------+------+-----------------+
| name| age|salary| join_date|gender|join_date_plus_30|
+-----+----+------+----------+------+-----------------+
|Alice|  25| 50000|2023-01-15|     F|       2023-02-14|
|  Bob|  30| 60000|2022-06-20|     M|       2022-07-20|
|Cathy|  28| 75000|2021-09-10|     F|       2021-10-10|
|David|null| 45000|2020-03-05|     M|       2020-04-04|
+-----+----+------+----------+------+-----------------+

6. Aggregation with expr()
# Calculate average salary by gender
df_agg = df.groupBy("gender").agg(expr("AVG(salary) AS avg_salary"))
df_agg.show()
+------+----------+
|gender|avg_salary|
+------+----------+
|     F|   62500.0|
|     M|   52500.0|
+------+----------+

7. Using expr() in Filters
# Filter employees with salary > 55000
df_filter = df.where(expr("salary > 55000"))
df_filter.show()
+-----+---+------+----------+------+
| name|age|salary| join_date|gender|
+-----+---+------+----------+------+
|  Bob| 30| 60000|2022-06-20|     M|
|Cathy| 28| 75000|2021-09-10|     F|
+-----+---+------+----------+------+

8. Mathematical Functions
# Round salary to nearest 1000
df_math = df.withColumn("salary_rounded", expr("ROUND(salary, -3)"))
df_math.show()
+-----+----+------+----------+------+--------------+
| name| age|salary| join_date|gender|salary_rounded|
+-----+----+------+----------+------+--------------+
|Alice|  25| 50000|2023-01-15|     F|       50000.0|
|  Bob|  30| 60000|2022-06-20|     M|       60000.0|
|Cathy|  28| 75000|2021-09-10|     F|       75000.0|
|David|null| 45000|2020-03-05|     M|       45000.0|
+-----+----+------+----------+------+--------------+

9. Array and Struct Operations
# Create an array column and extract first element
df_array = df.withColumn("name_array", expr("ARRAY(name, gender)"))
df_array = df_array.withColumn("first_element", expr("name_array[0]"))
df_array.show()
+-----+----+------+----------+------+------------+-------------+
| name| age|salary| join_date|gender|  name_array|first_element|
+-----+----+------+----------+------+------------+-------------+
|Alice|  25| 50000|2023-01-15|     F|[Alice, F]  |        Alice|
|  Bob|  30| 60000|2022-06-20|     M|[Bob, M]    |          Bob|
|Cathy|  28| 75000|2021-09-10|     F|[Cathy, F]  |        Cathy|
|David|null| 45000|2020-03-05|     M|[David, M]  |        David|
+-----+----+------+----------+------+------------+-------------+

10. Combining Multiple Operations
Combine multiple SQL operations in a single expr().

# Combine arithmetic, conditional, and string operations
df_combined = df.withColumn(
    "summary",
    expr("""
        CONCAT(
            name, 
            ' has salary ', 
            CASE 
                WHEN salary > 70000 THEN 'High'
                ELSE 'Normal'
            END,
            ' and age ', 
            COALESCE(age, 0)
        )
    """)
)
df_combined.show(truncate=False)
+-----+----+------+----------+------+--------------------------------+
|name |age |salary|join_date |gender|summary                         |
+-----+----+------+----------+------+--------------------------------+
|Alice|25  |50000 |2023-01-15|F     |Alice has salary Normal and age 25|
|Bob  |30  |60000 |2022-06-20|M     |Bob has salary Normal and age 30  |
|Cathy|28  |75000 |2021-09-10|F     |Cathy has salary High and age 28  |
|David|null|45000 |2020-03-05|M     |David has salary Normal and age 0 |
+-----+----+------+----------+------+--------------------------------+

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> SELECTEXPR
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Differences Between expr() and selectExpr()
expr(): Returns a Column object for use in withColumn(), filter(), or other operations.
selectExpr(): Directly selects or transforms columns into a new DataFrame, replacing the need for select(expr(...)).
Example equivalence: df.select(expr("salary * 1.10 AS salary_with_bonus")) is the same as df.selectExpr("salary * 1.10 AS salary_with_bonus").

---

# Select name and salary, rename salary to employee_salary
df_select = df.selectExpr("name", "salary AS employee_salary")
df_select.show()

---
# Calculate salary with a 10% bonus
df_arithmetic = df.selectExpr("*", "salary * 1.10 AS salary_with_bonus")
df_arithmetic.show()
---
# Concatenate name and gender, convert name to uppercase
df_string = df.selectExpr(
    "UPPER(name) AS name_upper",
    "CONCAT(name, ' - ', gender) AS name_gender"
)
df_string.show()
---
# Categorize salary into Low, Medium, High
df_conditional = df.selectExpr(
    "name",
    "salary",
    "CASE WHEN salary < 50000 THEN 'Low' " +
    "WHEN salary BETWEEN 50000 AND 70000 THEN 'Medium' " +
    "ELSE 'High' END AS salary_category"
)
df_conditional.show()
---
# Replace null age with 27
df_null = df.selectExpr("*", "COALESCE(age, 27) AS age_filled")
df_null.show()
---
# Add 30 days to join_date and extract year
df_date = df.selectExpr(
    "name",
    "join_date",
    "DATE_ADD(join_date, 30) AS join_date_plus_30",
    "YEAR(join_date) AS join_year"
)
df_date.show()
---
# Round salary to nearest 1000
df_math = df.selectExpr("name", "salary", "ROUND(salary, -3) AS salary_rounded")
df_math.show()
---
# Create an array and extract first element
df_array = df.selectExpr(
    "name",
    "ARRAY(name, gender) AS name_gender_array",
    "ARRAY(name, gender)[0] AS first_element"
)
df_array.show()
---
# Combine arithmetic, conditional, and string operations
df_combined = df.selectExpr(
    "name",
    "CONCAT(name, ' has salary ', " +
    "CASE WHEN salary > 70000 THEN 'High' ELSE 'Normal' END, " +
    "' and age ', COALESCE(age, 0)) AS summary"
)
df_combined.show(truncate=False)
---
# Calculate average salary by gender
df_agg = df.groupBy("gender").selectExpr("gender", "AVG(salary) AS avg_salary")
df_agg.show()
---

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> COLUMN, ARRAY
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, col, concat_ws

# Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data
data = [
    (101, 87),
    (102, 90),
    (101, 78),
    (103, 88),
    (102, 85),
]

# Create DataFrame
df = spark.createDataFrame(data, ["StudentID", "Marks"])

# Group and collect marks into comma-separated string
result = df.groupBy("StudentID") \
    .agg(concat_ws(",", collect_list(col("Marks"))).alias("Marks_List"))

result.show()
+---------+----------+
|StudentID|Marks_List|
+---------+----------+
|      101|   87,78  |
|      102|   90,85  |
|      103|     88   |
+---------+----------+

---
from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, col, collect_list, map_from_entries, to_json

# Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data
data = [
    (101, "Hindi", 87),
    (101, "English", 78),
    (101, "Math", 65),
    (102, "Hindi", 90),
    (102, "English", 85),
]

# Create DataFrame
df = spark.createDataFrame(data, ["StudentID", "Subject", "Marks"])

# Group and build JSON dictionary
result = df.groupBy("StudentID").agg(
    to_json(
        map_from_entries(
            collect_list(
                struct(col("Subject"), col("Marks"))
            )
        )
    ).alias("Marks_Dict")
)

result.show(truncate=False)
+---------+------------------------------------------+
|StudentID|Marks_Dict                                |
+---------+------------------------------------------+
|101      |{"Hindi":87,"English":78,"Math":65}       |
|102      |{"Hindi":90,"English":85}                 |
+---------+------------------------------------------+

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> SIMPLE IF
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

spark = SparkSession.builder.getOrCreate()

# Sample data
data = [(1, 95), (2, 82), (3, 73), (4, 65), (5, 40)]
df = spark.createDataFrame(data, ["StudentID", "Marks"])

# Apply complex if-elif-else condition
df = df.withColumn("Grade",
    when(col("Marks") >= 90, "A+")
    .when(col("Marks") >= 80, "A")
    .when(col("Marks") >= 70, "B")
    .when(col("Marks") >= 60, "C")
    .otherwise("Fail")
)

df.show()
+----------+-----+-----+
|StudentID |Marks|Grade|
+----------+-----+-----+
|    1     |  95 | A+  |
|    2     |  82 | A   |
|    3     |  73 | B   |
|    4     |  65 | C   |
|    5     |  40 | Fail|
+----------+-----+-----+
> NESTED IF 
Senerio:
if Marks > 80:
    if Subject == "Math":
        if StudentID < 5:
            Remark = "Top Math Student"
        else:
            Remark = "Senior Math Star"
    else:
        Remark = "Excellent Non-Math Student"
else:
    Remark = "Needs Improvement"

Pyspark:
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "Math", 85),
    (2, "Math", 92),
    (5, "Math", 95),
    (3, "Science", 88),
    (4, "English", 75),
    (6, "Science", 60),
]

df = spark.createDataFrame(data, ["StudentID", "Subject", "Marks"])

# Deep nested if-else using when
df = df.withColumn("Remark",
    when(col("Marks") > 80,
         when(col("Subject") == "Math",
              when(col("StudentID") < 5, "Top Math Student")
              .otherwise("Senior Math Star")
         ).otherwise("Excellent Non-Math Student")
    ).otherwise("Needs Improvement")
)

df.show(truncate=False)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> FILTER
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Initialize Spark session
spark = SparkSession.builder.appName("ComplexFilterExamples").getOrCreate()

# Sample data
data = [
    ("Alice", 25, 50000, "2023-01-15", "F"),
    ("Bob", 30, 60000, "2022-06-20", "M"),
    ("Cathy", 28, 75000, "2021-09-10", "F"),
    ("David", None, 45000, "2020-03-05", "M")
]
columns = ["name", "age", "salary", "join_date", "gender"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
df.show()
+-----+----+------+----------+------+
| name| age|salary| join_date|gender|
+-----+----+------+----------+------+
|Alice|  25| 50000|2023-01-15|     F|
|  Bob|  30| 60000|2022-06-20|     M|
|Cathy|  28| 75000|2021-09-10|     F|
|David|null| 45000|2020-03-05|     M|
+-----+----+------+----------+------+
---

# Using expr() with filter()
df_filter1 = df.filter(
    expr("(gender = 'F' AND salary > 55000) OR (age < 30 AND join_date > '2021-01-01')")
)
df_filter1.show()
+-----+---+------+----------+------+
| name|age|salary| join_date|gender|
+-----+---+------+----------+------+
|Alice| 25| 50000|2023-01-15|     F|
|Cathy| 28| 75000|2021-09-10|     F|
+-----+---+------+----------+------+
Explanation:

(gender = 'F' AND salary > 55000): Matches Cathy (female, salary 75000).
(age < 30 AND join_date > '2021-01-01'): Matches Alice (age 25, joined 2023) and Cathy (age 28, joined 2021).
The OR combines these, and the result includes both Alice and Cathy.
Native PySpark Equivalent:

from pyspark.sql.functions import col
df_filter1_native = df.filter(
    ((col("gender") == "F") & (col("salary") > 55000)) |
    ((col("age") < 30) & (col("join_date") > "2021-01-01"))
)
df_filter1_native.show()

2. Using CASE WHEN in selectExpr() with Filtering
Use selectExpr() to create a new column based on complex conditions and then filter rows based on that column.
# Create a category column and filter rows where category is 'HighValue'
df_filter2 = df.selectExpr(
    "*",
    "CASE WHEN salary > 60000 OR (age IS NOT NULL AND age >= 28) THEN 'HighValue' ELSE 'Standard' END AS employee_category"
).filter(expr("employee_category = 'HighValue'"))
df_filter2.show()
+-----+---+------+----------+------+----------------+
| name|age|salary| join_date|gender|employee_category|
+-----+---+------+----------+------+----------------+
|  Bob| 30| 60000|2022-06-20|     M|       HighValue|
|Cathy| 28| 75000|2021-09-10|     F|       HighValue|
+-----+---+------+----------+------+----------------+
Explanation:

selectExpr() creates a new column employee_category:
HighValue if salary > 60000 or age is not null and >= 28.
Standard otherwise.
The filter() selects rows where employee_category = 'HighValue', matching Bob (age 30) and Cathy (salary 75000, age 28).

3. Pattern Matching with LIKE or RLIKE
Filter rows where the name contains a specific pattern or matches a regular expression.
# Filter names starting with 'A' or containing 'th' and salary >= 50000
df_filter3 = df.filter(
    expr("name LIKE 'A%' OR name RLIKE '.*th.*' AND salary >= 50000")
)
df_filter3.show()
+-----+---+------+----------+------+
| name|age|salary| join_date|gender|
+-----+---+------+----------+------+
|Alice| 25| 50000|2023-01-15|     F|
|Cathy| 28| 75000|2021-09-10|     F|
+-----+---+------+----------+------+
Explanation:

name LIKE 'A%': Matches names starting with 'A' (Alice).
name RLIKE '.*th.*': Matches names containing 'th' (Cathy).
AND salary >= 50000: Ensures salary is at least 50000.
The OR combines the name conditions, and the AND applies the salary condition.
Native PySpark Equivalent:

df_filter3_native = df.filter(
    (col("name").like("A%") | col("name").rlike(".*th.*")) & (col("salary") >= 50000)
)
df_filter3_native.show()

4. Date-Based Filtering with Complex Logic
Filter rows based on date conditions, such as employees who joined within a specific date range or have a specific tenure.

# Filter employees who joined between 2021 and 2023 and are younger than 30 or have null age
df_filter4 = df.filter(
    expr("join_date BETWEEN '2021-01-01' AND '2023-12-31' AND (age < 30 OR age IS NULL)")
)
df_filter4.show()
+-----+---+------+----------+------+
| name|age|salary| join_date|gender|
+-----+---+------+----------+------+
|Alice| 25| 50000|2023-01-15|     F|
|Cathy| 28| 75000|2021-09-10|     F|
+-----+---+------+----------+------+
Explanation:

join_date BETWEEN '2021-01-01' AND '2023-12-31': Matches Alice (2023-01-15) and Cathy (2021-09-10).
(age < 30 OR age IS NULL): Matches Alice (age 25), Cathy (age 28), and excludes David (null age, but joined in 2020).

5. Nested Conditions with Null Handling
Filter rows with nested conditions, handling null values explicitly.
# Filter where age is null or (salary > 50000 and gender is 'M')
df_filter5 = df.filter(
    expr("age IS NULL OR (salary > 50000 AND gender = 'M')")
)
df_filter5.show()
+-----+----+------+----------+------+
| name| age|salary| join_date|gender|
+-----+----+------+----------+------+
|  Bob|  30| 60000|2022-06-20|     M|
|David|null| 45000|2020-03-05|     M|
+-----+----+------+----------+------+
Explanation:

age IS NULL: Matches David.
(salary > 50000 AND gender = 'M'): Matches Bob.
The OR combines these, including both Bob and David.

6. Combining selectExpr() with Complex Filtering
Use selectExpr() to create derived columns and then apply a complex filter.
# Create a tenure column and filter employees with tenure > 1 year and high salary or young age
df_filter6 = df.selectExpr(
    "*",
    "DATEDIFF(CURRENT_DATE, join_date) / 365 AS tenure_years"
).filter(
    expr("tenure_years > 1 AND (salary > 60000 OR age < 28)")
)
df_filter6.show()
+-----+---+------+----------+------+------------------+
| name|age|salary| join_date|gender|      tenure_years|
+-----+---+------+----------+------+------------------+
|Cathy| 28| 75000|2021-09-10|     F|3.857534246575342|
+-----+---+------+----------+------+------------------+
Explanation:

DATEDIFF(CURRENT_DATE, join_date) / 365: Calculates tenure in years.
tenure_years > 1: Filters employees with more than 1 year of tenure (excludes Alice, Bob, includes Cathy, David).
(salary > 60000 OR age < 28): Matches Cathy (salary 75000) but not David (age null, salary 45000).

7. Filtering with Array and Struct Operations
Filter based on complex types like arrays.
# Create an array and filter based on array conditions
df_filter7 = df.selectExpr(
    "*",
    "ARRAY(name, gender) AS name_gender_array"
).filter(
    expr("name_gender_array[1] = 'F' AND salary >= 50000")
)
df_filter7.show()
+-----+----+------+----------+------+-----------------+
| name| age|salary| join_date|gender|name_gender_array|
+-----+----+------+----------+------+-----------------+
|Alice|  25| 50000|2023-01-15|     F|     [Alice, F]  |
|Cathy|  28| 75000|2021-09-10|     F|     [Cathy, F]  |
+-----+----+------+----------+------+-----------------+

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> WITHCOLUMNS
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# Using withColumns to add multiple columns
df_withColumns_simple = df.withColumns({
    "salary_with_bonus": exprprud expr("salary * 1.10"),
    "current_date": lit("2025-07-20")
})
df_withColumns_simple.show()

> EXPR
# Using withColumn with a complex SQL expression
df_withColumn_complex = df.withColumn(
    "employee_summary",
    expr("""
        CONCAT(
            name, ' is ',
            CASE 
                WHEN salary > 60000 THEN 'high-paid'
                WHEN salary BETWEEN 50000 AND 60000 THEN 'medium-paid'
                ELSE 'low-paid'
            END,
            ' with tenure ',
            ROUND(DATEDIFF('2025-07-20', join_date) / 365, 1),
            ' years'
        )
    """)
).filter(expr("employee_summary LIKE '%high-paid%' OR age IS NULL"))
df_withColumn_complex.show(truncate=False)

---

# Using withColumns with multiple complex expressions
df_withColumns_complex = df.withColumns({
    "salary_category": expr("""
        CASE 
            WHEN salary > 60000 THEN 'High'
            WHEN salary BETWEEN 50000 AND 60000 THEN 'Medium'
            ELSE 'Low'
        END
    """),
    "tenure_years": expr("ROUND(DATEDIFF('2025-07-20', join_date) / 365, 1)"),
    "name_gender_array": expr("ARRAY(name, gender)"),
    "age_filled": expr("COALESCE(age, 27)")
}).filter(expr("salary_category = 'High' OR tenure_years > 4"))
df_withColumns_complex.show(truncate=False)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> EXPR SELECTEXPR
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

expr()	
Definition	A function that parses a SQL expression string and returns a Column object for use in DataFrame operations like withColumn(), filter(), or groupBy().	

selectExpr()
A DataFrame method that evaluates one or more SQL expressions and returns a new DataFrame with the results as columns.

# Using expr() with withColumn to add a single column
df_expr_simple = df.withColumn("salary_with_bonus", expr("salary * 1.10"))
df_expr_simple.show()

# Using selectExpr to select and transform columns
df_selectExpr_simple = df.selectExpr("name", "salary * 1.10 AS salary_with_bonus")
df_selectExpr_simple.show()
---

# Using selectExpr for complex column transformations, followed by filtering
df_selectExpr_complex = df.selectExpr(
    "name",
    "salary",
    "CASE WHEN salary > 60000 THEN 'High' WHEN salary BETWEEN 50000 AND 60000 THEN 'Medium' ELSE 'Low' END AS salary_category",
    "ROUND(DATEDIFF('2025-07-20', join_date) / 365, 1) AS tenure_years",
    "ARRAY(name, gender) AS name_gender_array"
).filter(
    expr("salary_category = 'High' OR tenure_years > 4")
)
df_selectExpr_complex.show(truncate=False)

---
# Using expr() for complex transformations and filtering
df_expr_complex = df.withColumn(
    "employee_summary",
    expr("""
        CONCAT(
            name, ' is ',
            CASE 
                WHEN salary > 60000 THEN 'high-paid'
                WHEN salary BETWEEN 50000 AND 60000 THEN 'medium-paid'
                ELSE 'low-paid'
            END,
            ' with tenure ',
            ROUND(DATEDIFF('2025-07-20', join_date) / 365, 1),
            ' years'
        )
    """)
).filter(
    expr("employee_summary LIKE '%high-paid%' OR age IS NULL")
).withColumn(
    "age_filled",
    expr("COALESCE(age, 27)")
)
df_expr_complex.show(truncate=False)

> KEY

data = [
    ("Alice", 25, 50000, "2023-01-15", "F"),
    ("David", None, 45000, "2020-03-05", "M")
]
+-----+----+------+----------+------+
| name| age|salary| join_date|gender|
+-----+----+------+----------+------+
|Alice|  25| 50000|2023-01-15|     F|
|David|null| 45000|2020-03-05|     M|
+-----+----+------+----------+------+

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> ORDERBY
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

df_orderBy_simple = df.orderBy("salary")
df_orderBy_simple.show()

---

df.orderBy(col("gender"), col("salary").desc())

---

df_ordered = df.orderBy(
    col("gender").asc(),                        # sort gender A-Z (F first, then M)
    col("salary").desc(),                       # then by salary high to low
    (col("salary") * col("experience")).desc()  # then by total cost-to-company (computed)
)

df_ordered.show()
+-----+------+------+----------+
| name|gender|salary|experience|
+-----+------+------+----------+
|  Eve|     F|  1500|         1|
|Alice|     F|  1000|         3|
|Carol|     F|  1000|         5|
|  Bob|     M|  1500|         2|
|David|     M|  1200|         4|
+-----+------+------+----------+

---

# Using orderBy with expr() and selectExpr
df_orderBy_complex = df.selectExpr(
    "*",
    "ROUND(DATEDIFF('2025-07-20', join_date) / 365, 1) AS tenure_years",
    "CASE WHEN salary > 60000 THEN 'High' WHEN salary BETWEEN 50000 AND 60000 THEN 'Medium' ELSE 'Low' END AS salary_category"
).orderBy(
    expr("tenure_years DESC"),
    expr("salary_category ASC")
)
df_orderBy_complex.show(truncate=False)

# Using sort with expr() and selectExpr
df_sort_complex = df.selectExpr(
    "*",
    "ROUND(DATEDIFF('2025-07-20', join_date) / 365, 1) AS tenure_years",
    "CASE WHEN salary > 60000 THEN 'High' WHEN salary BETWEEN 50000 AND 60000 THEN 'Medium' ELSE 'Low' END AS salary_category"
).sort(
    expr("tenure_years DESC"),
    expr("salary_category ASC")
)
df_sort_complex.show(truncate=False)

---

# Using orderBy with expr() for conditional sorting
df_orderBy_null = df.selectExpr(
    "*",
    "ROUND(DATEDIFF('2025-07-20', join_date) / 365, 1) AS tenure_years"
).withColumn(
    "salary_per_year",
    expr("ROUND(salary / tenure_years, 2)")
).orderBy(
    expr("CASE WHEN age IS NULL THEN 1 ELSE 0 END"),  # Null ages come last
    expr("salary_per_year DESC")
).filter(expr("tenure_years > 2"))
df_orderBy_null.show(truncate=False)


# Using sort with expr() for conditional sorting
df_sort_null = df.selectExpr(
    "*",
    "ROUND(DATEDIFF('2025-07-20', join_date) / 365, 1) AS tenure_years"
).withColumn(
    "salary_per_year",
    expr("ROUND(salary / tenure_years, 2)")
).sort(
    expr("CASE WHEN age IS NULL THEN 1 ELSE 0 END"),  # Null ages come last
    expr("salary_per_year DESC")
).filter(expr("tenure_years > 2"))
df_sort_null.show(truncate=False)


---

df.orderBy("gender", col("salary").desc())
"gender" is passed as a string — PySpark automatically treats it as a column.

col("salary").desc() is a Column expression — used to specify descending order.


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> WINDOW 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Window Specification: Defined using Window.partitionBy() (grouping) and Window.orderBy() (ordering within the partition). Optional rowsBetween() or rangeBetween() defines the window frame.
Types of Window Functions:
Ranking Functions: row_number(), rank(), dense_rank(), ntile(), percent_rank().
Aggregate Functions: sum(), avg(), min(), max(), etc., over a window.
Value Functions: lag(), lead(), first(), last().
Analytic Functions: cume_dist(), stddev(), etc.
Usage: Applied using over(window) with a column expression, often via withColumn() or expr().

---

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, rank, dense_rank, sum, avg, lag, lead, max, min
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

data = [
    ("Sales", "Alice", 2024, 1000),
    ("Sales", "Bob",   2024, 1200),
    ("Sales", "Alice", 2025, 1500),
    ("HR",    "David", 2024, 1100),
    ("HR",    "Eve",   2025, 1300),
    ("HR",    "Eve",   2024, 1400),
    ("HR",    "David", 2025, 1250),
]

df = spark.createDataFrame(data, ["dept", "employee", "year", "salary"])
df.show()

---

windowSpec = Window.orderBy("salary")
df.withColumn("row_num", row_number().over(windowSpec)).show()

---

df.withColumn("rank", rank().over(windowSpec)).show()
df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> WINDOW, RANK, DENSE_RANK
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


from pyspark.sql.functions import least, greatest

df = spark.createDataFrame([
    ("A", 10, 20, 30),
    ("B", 50, 40, 60),
    ("C", 5, 25, 15)
], ["name", "val1", "val2", "val3"])

df = df.withColumn("min_val", least("val1", "val2", "val3")) \
       .withColumn("max_val", greatest("val1", "val2", "val3"))

df.show()
+-----+-----+-----+-----+--------+--------+
|name |val1 |val2 |val3 |min_val |max_val |
+-----+-----+-----+-----+--------+--------+
|A    |10   |20   |30   |10      |30      |
|B    |50   |40   |60   |40      |60      |
|C    |5    |25   |15   |5       |25      |
+-----+-----+-----+-----+--------+--------+

---

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("RankVsDenseRank").getOrCreate()

# Sample data with tied salaries
data = [
    ("Alice", 25, 50000, "2023-01-15", "F", "HR"),
    ("Bob", 30, 60000, "2022-06-20", "M", "IT"),
    ("Cathy", 28, 60000, "2021-09-10", "F", "HR"),
    ("David", None, 45000, "2020-03-05", "M", "IT"),
    ("Eve", 27, 50000, "2022-12-01", "F", "Finance"),
    ("Frank", 32, 80000, "2021-03-15", "M", "Finance")
]
columns = ["name", "age", "salary", "join_date", "gender", "department"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
df.show(truncate=False)
+-----+----+------+----------+------+----------+
|name |age |salary|join_date |gender|department|
+-----+----+------+----------+------+----------+
|Alice|25  |50000 |2023-01-15|F     |HR        |
|Bob  |30  |60000 |2022-06-20|M     |IT        |
|Cathy|28  |60000 |2021-09-10|F     |HR        |
|David|null|45000 |2020-03-05|M     |IT        |
|Eve  |27  |50000 |2022-12-01|F     |Finance   |
|Frank|32  |80000 |2021-03-15|M     |Finance   |
+-----+----+------+----------+------+----------+

# Define window: order by salary (no partitioning)
window_spec = Window.orderBy(col("salary").desc())

# Add RANK and DENSE_RANK
df_medium = df.selectExpr(
    "name",
    "salary",
    "RANK() OVER (ORDER BY salary DESC) AS rank",
    "DENSE_RANK() OVER (ORDER BY salary DESC) AS dense_rank"
).orderBy(col("salary").desc())
df_medium.show(truncate=False)

+-----+------+----+----------+
|name |salary|rank|dense_rank|
+-----+------+----+----------+
|Frank|80000 |1   |1         |
|Bob  |60000 |2   |2         |
|Cathy|60000 |2   |2         |
|Alice|50000 |4   |3         |
|Eve  |50000 |4   |3         |
|David|45000 |6   |4         |
+-----+------+----+----------+

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> LOG, LOGGER, AWS LOGGER
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("LogWithCounter").getOrCreate()

log_path = "s3a://your-bucket/logs/"  # or "/tmp/logs"
columns = ["log_id", "timestamp", "status", "message"]

# Counter (global)
log_counter = 0
is_first = True  # To write header only once

def log_event(status, message):
    global log_counter, is_first

    log_counter =  log_counter + 1
    row = [(log_counter, datetime.now().isoformat(), status, message)]
    df = spark.createDataFrame(row, schema=columns)
    df.write.csv(log_path, mode="append", header=is_first)
    is_first = False

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> RDD 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark create RDD example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.sparkContext\
    .parallelize([
        (1, 2, 3, 'a b c'),
        (4, 5, 6, 'd e f'),
        (7, 8, 9, 'g h i')
    ])\
    .toDF(['col1', 'col2', 'col3', 'col4'])

df.show()

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> IF, ELSE, CONDITION, WHEN, OTHERWISE 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

If person is an Indian citizen:
    If person belongs to any Indian state:
        If age ≥ 18:
            If category is SC or ST:
                If state is "Uttar Pradesh" or "Bihar":
                    benefit_amount = 25000
                Else:
                    benefit_amount = 20000
            Else:
                benefit_amount = 10000
        Else:
            If category is OBC or category is SC:
                benefit_amount = 5000
            Else:
                benefit_amount = 0
    Else:
        If category is SC or ST:
            benefit_amount = 8000
        Else:
            benefit_amount = 0
Else:
    If state is in India and category is SC or ST:
        benefit_amount = 12000
    Else:
        If country is "USA":
            benefit_amount = 100
        ELIf country is "Greenland":
            benefit_amount = 200
        ELIf country is "China":
            benefit_amount = 300
        ELIf country is "Russia":
            benefit_amount = 500

---

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("BenefitCalculation").getOrCreate()

data = [
    ("Indian", "Uttar Pradesh", 19, "SC", "India"),
    ("Indian", "Bihar", 20, "ST", "India"),
    ("Indian", "Kerala", 22, "GEN", "India"),
    ("Indian", "Tamil Nadu", 16, "OBC", "India"),
    ("Indian", "Rajasthan", 17, "SC", "India"),
    ("Indian", "Maharashtra", 15, "GEN", "India"),
    ("Indian", "Uttar Pradesh", 30, "GEN", "India"),
    ("Indian", "Delhi", 25, "OBC", "India"),
    ("Indian", "Haryana", 40, "ST", "India"),
    ("Indian", "Unknown", 19, "SC", "India"),
    ("Indian", "Unknown", 15, "GEN", "India"),
    ("Foreigner", "Delhi", 21, "SC", "India"),
    ("Foreigner", "Mumbai", 22, "GEN", "India"),
    ("Foreigner", "N/A", 30, "GEN", "USA"),
    ("Foreigner", "N/A", 28, "GEN", "Greenland"),
    ("Foreigner", "N/A", 29, "GEN", "China"),
    ("Foreigner", "N/A", 31, "GEN", "Russia"),
    ("Foreigner", "N/A", 33, "GEN", "UK"),
    ("Foreigner", "Bihar", 26, "ST", "India"),
    ("Foreigner", "Kolkata", 25, "SC", "India"),
    ("Indian", "Punjab", 16, "SC", "India"),
    ("Indian", "Bihar", 16, "GEN", "India"),
    ("Indian", "Unknown", 18, "GEN", "India"),
    ("Indian", "Jharkhand", 20, "ST", "India"),
]

columns = ["citizenship", "state", "age", "category", "country"]
df = spark.createDataFrame(data, columns)

indian_states = ["Uttar Pradesh", "Bihar", "Kerala", "Tamil Nadu", "Delhi", "Maharashtra", "Rajasthan", "Punjab", "Haryana", "Jharkhand"]

df = df.withColumn(
    "benefit_amount",
    when(col("citizenship") == "Indian",
         when(col("state").isin(indian_states),
              when(col("age") >= 18,
                   when(col("category").isin("SC", "ST"),
                        when(col("state").isin("Uttar Pradesh", "Bihar"), 25000)
                        .otherwise(20000))
                   .otherwise(10000))
              .otherwise(
                  when(col("category").isin("SC", "OBC"), 5000)
                  .otherwise(0))
              )
         .otherwise(
             when(col("category").isin("SC", "ST"), 8000)
             .otherwise(0))
         )
    .otherwise(
        when((col("state").isin(indian_states)) & (col("category").isin("SC", "ST")), 12000)
        .otherwise(
            when(col("country") == "USA", 100)
            .when(col("country") == "Greenland", 200)
            .when(col("country") == "China", 300)
            .when(col("country") == "Russia", 500)
            .otherwise(0)
        )
    )
)

df.show(truncate=False)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> LOG, LOGGER, LOGGER IN S3 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import lit, current_timestamp

def write2log(spark, action_type, message, activity_name, s3_path):
    mode = "overwrite" if action_type == "INIT" else "append"

    schema = StructType([
        StructField("activity_name", StringType(), True),
        StructField("action_type", StringType(), True),
        StructField("message", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    log_df = spark.createDataFrame(
        [(activity_name, action_type, message)],
        schema=schema
    ).withColumn("timestamp", current_timestamp())

    log_df.write.mode(mode).format("csv").option("header", True).save(s3_path)

=========================================================

write2log(
    spark,
    action_type="INIT",
    message="Pipeline started",
    activity_name="load_vehicle_data",
    s3_path="s3://my-bucket/logs/vehicle_pipeline/"
)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> COST, GLU 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Key Factors Influencing Cost

Pricing Model:
AWS Glue: A serverless ETL service charged based on Data Processing Units (DPUs) per hour. A DPU is roughly equivalent to 4 vCPUs and 16 GB of memory, priced at $0.44 per DPU-hour (as of the referenced data) with a 10-minute minimum billing duration for standard execution. Flexible execution (FLEX) is cheaper but unsuitable for time-sensitive workloads due to potential interruptions. Additional costs may arise from Data Catalog storage ($1 per 100,000 objects per month) and crawler runs ($0.44 per DPU-hour, minimum 2 DPUs).
Databricks: Pricing includes Databricks Units (DBUs) plus the cost of underlying AWS EC2 instances. DBUs vary by compute type (e.g., Jobs Compute for ETL) and pricing tier (e.g., Premium). For example, Jobs Compute on AWS might cost $0.15–$0.40 per DBU-hour, depending on the instance type and region, with EC2 costs added separately (e.g., ~$0.10–$0.20 per hour for an m5.xlarge instance). Spot instances can reduce EC2 costs significantly. Databricks also supports autoscaling, which can optimize resource usage.
---
Workload Characteristics:
Data Volume and Processing Time: Larger datasets or complex transformations increase processing time, impacting costs. Databricks may perform better for large-scale or complex workloads due to optimizations like the Photon engine, but performance varies by use case.
Job Frequency and Duration: Glue’s 10-minute minimum billing can inflate costs for short jobs, while Databricks’ per-second billing (after a 1-minute minimum) is more flexible for variable workloads.
Optimization Features: Databricks’ Photon engine and Delta Lake can reduce processing times, potentially lowering costs. Glue offers serverless scalability but may require custom tuning for optimal performance.
---
Infrastructure Management:
AWS Glue: Fully managed, requiring no infrastructure setup. This reduces operational overhead but limits control over Spark configurations.
Databricks: Requires managing EC2 instances (or using serverless options), which adds complexity but allows fine-tuned optimizations, such as using spot instances or Graviton processors.
---
Additional Costs:
Glue: Data Catalog storage, crawler runs, and integration with other AWS services (e.g., S3, Redshift) may add costs.
Databricks: Includes costs for notebooks, storage (e.g., DBFS), and optional features like Delta Live Tables or MLflow. EC2 instance costs are separate and can vary based on instance type and spot pricing.
---
Cost Comparison Example
Let’s consider a sample PySpark job reading and processing a 225 GB CSV file daily for 30 days, performing aggregations and joins, based on referenced data.
---
AWS Glue

Setup: Assume 5 DPUs (equivalent to m4.xlarge, 4 vCPUs, 16 GB memory) for a job taking 1 hour per day.
Cost Calculation:

Glue ETL: 5 DPUs × 30 hours × $0.44/DPU-hour = $66/month.
Data Catalog: Assuming <100,000 objects, free under AWS Free Tier. Crawler runs (e.g., 2 DPUs × 0.5 hours × $0.44 × 30) add ~$13.20/month.
Total: ~$79.20/month (excluding S3 storage or other AWS service costs).
---
Performance Note: Glue’s serverless nature simplifies setup, but performance may lag for complex jobs without optimization. For a 225 GB file, Glue might take longer if not tuned properly.

Databricks
Setup: Assume an equivalent cluster (e.g., 5 m4.xlarge instances) running for 1 hour daily, using Jobs Compute (Premium tier, ~$0.20/DBU-hour) and EC2 costs (~$0.192/hour per m4.xlarge, or lower with spot instances at ~$0.06/hour).
Cost Calculation:

DBUs: Assume 1 DBU per m4.xlarge per hour, so 5 DBUs × 30 hours × $0.20 = $30.
EC2 (on-demand): 5 instances × 30 hours × $0.192 = $28.80.
EC2 (spot, ~70% discount): 5 instances × 30 hours × $0.06 = $9.
Total: $59 (on-demand) or $39 (spot) per month (excluding storage or additional features).
---
Performance Note: Databricks took 1 hour 5 minutes for a 225 GB file, compared to EMR’s 40 minutes, suggesting potential for optimization (e.g., enabling Photon). Spot instances and autoscaling can further reduce costs.

Comparison
Glue: ~$79.20/month, simpler to manage, but less flexible for optimization.
Databricks: ~$39–$59/month, potentially cheaper with spot instances, but requires cluster management unless using serverless options.

Performance Considerations
Glue: May be slower for complex workloads (e.g., 225 GB CSV processing) due to default Spark configurations and lack of advanced optimizations like Photon. Users report needing custom transformations for basic operations, increasing development effort.
Databricks: Offers better performance for large-scale or complex jobs with Photon and Delta Lake, but a test showed it was slower than AWS EMR (1 hour 5 minutes vs. 40 minutes for 225 GB). Tuning cluster configurations or using Photon could close this gap.
---
Recommendations

Choose AWS Glue if:
Your workload is simple, and you prioritize minimal setup and management.
Jobs are short or infrequent, but note the 10-minute minimum billing.
You’re heavily integrated with AWS services (e.g., S3, Redshift, Athena).
Example: Small-scale ETL jobs with straightforward transformations.
---
Choose Databricks if:
You process large datasets or complex transformations where Photon or Delta Lake optimizations can reduce runtime.
You can leverage spot instances or autoscaling to lower EC2 costs.
You need advanced analytics, machine learning, or collaborative features (e.g., notebooks).
Example: Large-scale data processing or workflows requiring ML integration.
---
Cost Optimization Tips:
Glue: Use FLEX execution for non-urgent jobs, minimize crawler runs, and optimize Spark code to reduce DPU usage.
Databricks: Use spot instances, enable autoscaling, and leverage Photon for faster processing. Monitor DBU and EC2 usage to avoid overprovisioning.
---
Testing: Run a proof-of-concept (POC) with your specific workload to compare runtime and costs, as performance varies by data size, complexity, and optimization.

Conclusion
Databricks is likely more cost-effective for large-scale or complex PySpark workloads, especially with spot instances and Photon, potentially costing $39–$59/month for the example job compared to Glue’s ~$79.20/month. However, Glue is simpler and may be cheaper for smaller, less frequent jobs with minimal management needs. To confirm, test both services with your actual PySpark code and data, as costs depend heavily on workload specifics. For Glue pricing details, visit https://aws.amazon.com/glue/pricing/. For Databricks pricing, check https://databricks.com/product/pricing.

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> REFERENCE
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
DECORATOR
    decorators are a highly effective and helpful python feature because we can use them to change the 
    behavior of a function or class without changing the existing method/process
	
#####

DECORATOR
    decorators are a highly effective and helpful python feature because we can use them to change the 
    behavior of a function or class without changing the existing method/process
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> DECORATOR
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
DECORATOR
    decorators are a highly effective and helpful python feature because we can use them to change the 
    behavior of a function or class without changing the existing method/process

    @<decorator name>

FUNCTION AS AN ARGUMENT

def outer_finction():
    print("start")
    def inner_function():
        print("Hello")

    return inner_function

my_func1=outer_finction()
my_func1()

#####


WRAPPER FUNCTION 

def decorator_function(original_function):

    def wrapper_function():
        print("before original function")
        return original_function()
    return wrapper_function()

def display():
    print("original function")

decorator_function(display)

#####

WRAPPER FUNCTION USING DECORATOR FUNCTION

def decorator_function(original_function):

    def wrapper_function():
        print("before original function")
        return original_function
    return wrapper_function()

@decorator_function
def display():
    print("original function")

display()

#####

WHERE TO USE
    high oder function - passing function as an argument
    seter or getter 	
    abstract function	
    flask route

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> ARGUMENTS, *ARG, ARG, **KWARGS, KWARGS
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Formal arguments
Actual arguments

def sum(a, b):
   c = a + b            # a and b are formal arguments
   print(c)
# call the function
x = 10
y = 15
sum(x, y)               # x and y are actual arguments

#####

Positional arguments
Keyword arguments
Default arguments
Variable-length arguments
keyword variable-length argument

#####

default argument value

def my_function(country = "Norway"):
    print("I am from " + country)

my_function("Sweden")
my_function()

#####

*arg

If you do not know how many arguments that will be passed into your function, add a * before the parameter name in the function definition.

This way the function will receive a tuple of arguments

def myFun(*arg):
    for arg in arg:
        print(arg)


myFun('Hello', 'Welcome', 'to', 'GeeksforGeeks', 10,20,30)

#####

**kwargs

If the number of keyword arguments is unknown, add a double ** before the parameter name:
This way the function will receive a dictionary of arguments

def myFunction(**kwargs):
    for key, value in kwargs.items():
        print(key, '-', value)

if __name__ == "__main__":
    myFunction(a = 24, b = 87, c = 3, d = 46)
 

Output
a - 24
b - 87
c - 3
d - 46

#####

def myFunction(**computers):
    for kw in computers:
        print(kw, '-', computers[kw])

if __name__ == "__main__":
    myFunction(dell = 1299.50, asus = 1870.00, hp = 1990.50)

Output
dell - 1299.5
asus - 1870.0
hp - 1990.5

#####

def myFunction(x, y, **kwargs):
    print(x)
    print(y)
    for key, value in kwargs.items():
        print(key, '-', value)

if __name__ == "__main__":
    myFunction("ABC", "MNO", a = 24, b = 87, c = 3, d = 46)

Output
ABC
MNO
a - 24
b - 87
c - 3
d - 46

#####

def myFunction(*args, **kwargs):
    print(args)
    print(kwargs)

if __name__ == "__main__":
    myFunction("hello", "mars", a = 24, b = 87, c = 3, d = 46)

Output
('hello', 'mars')

#####