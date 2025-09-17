
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
>> WHEN WITH SELECT
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
>>  
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

+++ Note
df1.ID works fine — it’s like a shortcut.

col("df2_ID") is used when the column is not directly referenced from a DataFrame object, or it's renamed.

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>>  
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
>> WITHCOLUMNS
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

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
+ DOCUMENT END 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
