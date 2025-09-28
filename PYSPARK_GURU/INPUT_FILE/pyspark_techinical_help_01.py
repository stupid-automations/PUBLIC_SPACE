
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> TOPIC
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  
Data Skipping Index
Z-Order
autoloader
batch processing
streaming processing
micro batch
skew
Cache or Persist
Catalyst Optimize
Column Pruning  - Meaning: Removing unnecessary columns early in the query planand Filter 
Pushdown  - So the filtering happens at the source, not after loading everything into Spark.

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> KEY
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  
from pyspark.sql import SparkSession 
spark = SparkSession.builder \
 .appName("MySparkApp") \
 .master("local[*]") \
 .getOrCreate() 

"local" means: run Spark on your local machine, not on a cluster (like YARN, Kubernetes, or Mesos).

"local[1]" → use only 1 core.
"local[2]" → use 2 cores.
"local[*]" → Spark will detect and use all cores.

---

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> INTERVIEW QUESTION, INTERVIEW, QUESTION, QUESTIONS
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Day to day basis how much volume of data u handle
what is the format of data?
on prem or cloud platform?
How u r scheduling yr jobs?
what's API u r using for real time data.
how much exp u have in Spark and how much in PySpark
explain architecture of Spark
how driver mode is assigned to machine, who takes care of that
diff bet wide transformations and narrow transformations 
diff bet transformation and action 
HDFS operations mutable or immutable
how u r comfy for coding in python
diff bet mutable and immutable datatypes in Python
diff bet SProc and Function 
what are indexes and what r diff type of indexes used in SQL
what r cursors in SQL
what r Set operators and diff types of Set operators used in SQL
R u aware of aggregate functions, pls tell me few of aggregate functions normally we use in most of the queries. 
diff bet delete and truncate command 
delete is DDL or DML command
u worked on scheduling tool Autosis, we use Autosis only -Can u tell me what is JILs (Job Information Language)
how do u stop or kill process in Autosis
diff bet oneyes and onhold autosis jobs
r u aware of CI/CD pipelines
wat r diff devops components/elements for CI/CD, what r steps performed for CI/CD
have u worked on CI/CD pipelines, r u aware of RLM (Release Management) process?
have u worked on any ETL tool, in our proj we use Talin
r u aware of Talin job design process
what's SCD (slowly Changing Dimension) logic in any ETL
if u r using ETL tool, how u handle data quality and data cleansing tasks
what is command to get no of partitions of a Dataframe
diff bet coalesce  and partitioning 
what r iterators u have used in Python 
Can u use DDL statements using PySpark or Spark

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> WHEN, OTHERWISE, GREATEST
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 
from pyspark.sql.functions import col, when, trim, greatest

df_return = df2.withColumn(
    "DRV_camp_open_st_dt",
    when(
            col("IN_camp_status") == "00",
            when(
                    ~col("IN_defect_code").startswith("STOP"),
                    when(
                            trim(col("VD_parameter_flag")) == "Y",
                            col("vd_camp_open_st_dt")
                    ).otherwise(
                            col("IN_camp_status_date").cast("date")
                    )
            ).otherwise(
                    when(
                            trim(col("VD_parameter_flag")) == "Y",
                            col("vd_camp_open_st_dt")
                    ).otherwise(
                            greatest(
                                col("DR_rec_start_date_mkt"),
                                col("IN_camp_status_date").cast("date")
                            )
                    )
            )
    ).otherwise(
        col("vd_camp_open_st_dt")
    )
)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> GREATEST, NULL, COMPARE
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 
data = [
    ("2024-01-01", "2024-02-01"),   # both non-null
    ("2024-01-01", None),           # one null
    (None, "2024-02-01"),           # one null
    (None, None)                    # both null
]

df = spark.createDataFrame(data, ["col1", "col2"]) \
          .withColumn("greatest_val", greatest(col("col1"), col("col2")))

df.show(truncate=False)

+----------+----------+------------+
|col1      |col2      |greatest_val|
+----------+----------+------------+
|2024-01-01|2024-02-01|2024-02-01  |
|2024-01-01|null      |2024-01-01  |
|null      |2024-02-01|2024-02-01  |
|null      |null      |null        |
+----------+----------+------------+

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> CMMIT, ROLLBACK, OPTIMIZATION, OPTIMIZE
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Right now you are using .collect() which pulls all 25,000 rows to the driver, and then running UPDATE one by one. 
That’s why it is very slow (40+ mins).
Instead, you should push the updates into executors using .foreachPartition().

def update_records(partition):
    import psycopg2

    conn = None
    try:
        conn = psycopg2.connect(
            dbname="your_db",
            user=postgres_user,
            password=postgres_password,
            host="your_host",
            port="your_port"
        )
        conn.autocommit = False
        cursor = conn.cursor()

        update_counter = 0

        for record in partition:
            update_counter += 1

            PV_update_insert_ind = record["IN_update_insert_ind"].strip()

            sql_query = f"""
                UPDATE ross.ross_vehicle_defects
                SET
                    camp_status_date      = {PV_camp_status_date}, 
                WHERE chassis_11_17   = '{PV_chassis_11_17}';
            """
            cursor.execute(sql_query)

            if update_counter % 1000 == 0:
                conn.commit()

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as err:
        if conn:
            conn.rollback()
        raise

# Run distributed
df_consolidate_5.foreachPartition(update_records) 
 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> GLUE IMPORT, IMPORT
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

https://files.pythonhosted.org/packages/cb/0e/bdc8274dc0585090b4e3432267d7be4dfbfd8971c0fa59167c711105a6bf/psycopg2-binary-2.9.10.tar.gz

### Create a zip file OR whl file and use 
import sys
sys.path.insert(0, 's3://your-bucket/path/psycopg2.zip')

import psycopg2

---

### Keep this file in s3 bucket and keep the path in Python Lib in GLUE advance properties in Job Details

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> CREATEORREPLACETEMPVIEW, TEMP VIEW, VIEW, CREATE
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++


from pyspark.sql import Row
# Create a static DataFrame
data = [
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Charlie", 28)
]

columns = ["id", "name", "age"]
df = spark.createDataFrame(data, columns)
df.createOrReplaceTempView("tmp_people")

result = spark.sql("SELECT id, name FROM tmp_people WHERE age > 26")
result.show()

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> QUERY, EXECUTION TIME, EXECUTION
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

import time
start_time = time.time()   
str_start_time = time.strftime("%H:%M:%S", time.localtime())

# Write Query OR Process Here

end_time = time.time()  
elapsed_time = end_time - start_time
print(f"Query execution time: {elapsed_time:.2f} seconds")

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> PARTITION, OPTIMIZE, OPTIMIZATION
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++


count_query = """
SELECT COUNT(*) AS row_count
FROM ross.ross_vehicle_defects
"""

count_df = spark.read.format("jdbc") \
    .option("url", postgres_jdbc_url) \
    .option("query", count_query) \
    .option("user", postgres_user) \
    .option("password", postgres_password) \
    .load()

max_rows = count_df.collect()[0]["row_count"]
print(f"Max rows in table: {max_rows}")

###########################


postgres_query = """
SELECT 
    ROW_NUMBER() OVER () AS rn,
    product_type    as VD_product_type, 
    defect_code     as VD_defect_code,
    vin_first_part  as VD_chassis_1_10, 
    chassis_11_17   as VD_chassis_11_17 
    FROM ross.ross_vehicle_defects 
""" 

df_VD = spark.read.format("jdbc") \
    .option("url", postgres_jdbc_url) \
    .option("dbtable", f"({postgres_query}) AS tmp") \
    .option("user", postgres_user) \
    .option("password", postgres_password) \
    .option("partitionColumn", "rn") \
    .option("lowerBound", "1") \
    .option("upperBound", str(max_rows)) \
    .option("numPartitions", "10") \
    .load()

print(f"vehicle_defects  : {df_VD.count()}")
df_VD.show(1, truncate=False)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> OPTIMIZE, OPTIMIZATION, BEST PRACTICE, BEST, PRACTICE
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

### If we use like this
UPDATE ross.ross_vehicle_defects
SET camp_status_date = {PV_camp_status_date}, ...

Problems with direct string formatting

SQL Injection risk — if any value contains quotes or special characters, it can break the query.

Performance — the database must parse each query as a new SQL string.

Errors with NULL or date values — e.g., Python None → SQL NULL may not be handled correctly.

Network overhead — each query is sent separately if not batched.


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> UPDATE PARAMETER, PARAMETER
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

"""
 update_parameter = [
     f"camp_status_date  = {PV_camp_status_date}",
     f"reserve_ag_dlr    = {PV_reserve_ag_dlr}",
     f"reserve_date      = {PV_reserve_date}",
     f"veh_insert_date   = {PV_veh_insert_date}",
     f"ssf_start_date    = {PV_ssf_date}",
     f"orig_dealer_no    = '{PV_orig_dealer_no}'",
     f"repair_claim_no   = '{PV_repair_claim_no}'",
     f"repair_dealer_no  = '{PV_repair_dealer_no}'",
     f"camp_status       = '{PV_camp_status}'",
     f"stop_sale_flag    = '{PV_stop_sale_flag}'",
     f"dwh_xmit_ind      = '{PV_dwh_xmit_ind}'",
     f"last_updt_process = '{DEF_last_updt_process}'",
     f"update_insert_ind = '{DEF_update_insert_ind}'",
     f"last_updt_tstmp   = '{DEF_last_updt_tstmp}'"
 ]
 
 ######################################
 # print(f"PV_camp_open_st_dt   :  {PV_camp_open_st_dt}")
       
 if PV_camp_open_st_dt != "'NO_UPD'":
     update_parameter.insert(5, f"camp_open_st_dt = {PV_camp_open_st_dt}")
 
 ######################################
 
 # Join all clauses with commas
 update_parameter_str = ",\n    ".join(update_parameter)
 
 sql_query = f
     UPDATE ross.ross_vehicle_defects
     SET
         {update_parameter_str}
     WHERE
         product_type = '{PV_product_type}'
         AND defect_code = '{PV_defect_code}'
         AND vin_first_part = '{PV_chassis_1_10}'
         AND chassis_11_17 = '{PV_chassis_11_17}';
        
"""
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> FIND DUPLICATE RECORD, DUPLICATE
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

df_unique = df_duplicate.dropDuplicates(["IN_product_type", "IN_defect_code", "IN_chassis_1_10", "IN_chassis_11_17"])
        
print(df_drop.count())

duplicate_record = df_duplicate.subtract(df_unique)

duplicate_record.show()

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> FILTER
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

filtered_df = df[df['RCVE.CAMP_STATUS_DATE'] <= df['DDR-RO-OPEN-DATE']]

filtered_df = df.query('`RCVE.CAMP_STATUS_DATE` <= `DDR-RO-OPEN-DATE`')

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> UPDATE TABLE FROM TEMPORARY TABLE, TEMPORARY, TEMP, UPDATE
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

conn = glueContext.spark_session._sc._gateway.jvm.java.sql.DriverManager.getConnection(
        postgres_jdbc_url, postgres_user, postgres_password
)
conn.setAutoCommit(False)
stmt = conn.createStatement()

#Create temp table
create_temp_sql = """
CREATE TEMP TABLE temp_mrt_task (
    id INTEGER
)
"""
stmt.execute(create_temp_sql)

#Insert a test record
insert_sql = """
INSERT INTO temp_mrt_task (id)
VALUES (219631)
"""
stmt.executeUpdate(insert_sql)  # use executeUpdate for INSERT/UPDATE/DELETE

#Select from temp table
select_sql = "SELECT * FROM temp_mrt_task"
rs = stmt.executeQuery(select_sql)

while rs.next():
    print(rs.getString("id"))


update_sql = """
UPDATE ROSS.ross_mrt_task m
SET status_master_id = '2'
FROM temp_mrt_task t
WHERE m.mr_task = t.id
"""
stmt.execute(update_sql)
conn.commit()

print("done.......")

rs.close()
stmt.close()
conn.close()


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> EXPLAIN PLAN, EXPLAIN, OPTIMIZATION, TUNNING
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

df_task.explain(True)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> UPDATE ON EXECUTOR USING LOOP, UPDATE, ITERATOR, COLLECT, PSYCOPG2
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    
import psycopg2

def write_to_db(iterator):
    conn = psycopg2.connect(
        host="localhost", dbname="mydb", user="myuser", password="mypassword"
    )
    cursor = conn.cursor()
    for row in iterator:
        cursor.execute("INSERT INTO my_table(id, value) VALUES (%s, %s)", (row[0], row[1]))
    conn.commit()
    cursor.close()
    conn.close()
    return []

df.rdd.foreachPartition(write_to_db)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> ROWNUM, COUNTER INCREMENT, COUNTER, INCREMENT, WINDOWS FUNCTION,WINDOW
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# Read max MR_TASK from the table
task_query = "SELECT MAX(MR_TASK) AS max_task FROM SERVICE.ROSS_MRT_TASK"

df_task = spark.read \
    .format("jdbc") \
    .option("url", postgres_jdbc_url) \
    .option("query", task_query) \
    .option("user", postgres_user) \
    .option("password", postgres_password) \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Convert to scalar
start_task = df_task.collect()[0]['max_task'] or 0  # if table empty, start from 1

#####

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, lit

# Add row numbers starting from 1
window_spec = Window.orderBy(lit(1))  # arbitrary ordering

df_main = df_main.withColumn("row_num", row_number().over(window_spec))

# Add task = start_task + row_num
df_main = df_main.withColumn("task", col("row_num") + lit(start_task))

# Drop temporary row_num
df_main = df_main.drop("row_num")

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> DBTABLE, TARGET TABLE
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

### Append to target table
df_main.write \
    .format("jdbc") \
    .option("url", postgres_jdbc_url) \
    .option("dbtable", "TARGET_SCHEMA.TARGET_TABLE") \
    .option("user", postgres_user) \
    .option("password", postgres_password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> UDF
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Python function
def greet(name):
    return f"Hello {name}"

# Register as UDF
greet_udf = udf(greet, StringType())

# Apply on DataFrame column
df.withColumn("greeting", greet_udf(df["name"])).show()

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> AWS DB2, DB2, CONFIG, 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

%%configure
{
  "--extra-jars": "s3://ross-library-files/db2_drivers/db2jcc4.jar,s3://ross-library-files/db2_drivers/db2jcc_license_cu-11.5.7.fp0.jar,s3://ross-library-files/db2_drivers/db2jcc_license_cisuz-jcc_v11_5_7_fp0.jar"
}

###########

try:  
    db2_username, db2_password, db2_url, db2_driver_name = get_credentials_db2('db2/prodzu')
    
    if not db2_username or not db2_password or not db2_url or not db2_driver_name:
        raise ValueError("db2 service credentials not found in Secrets Manager")
    
    # Add SSL to the URL
    db2_url_ssl = db2_url + ":sslConnection=true;"
    
    jvm = glueContext.spark_session._sc._gateway.jvm
    jvm.java.lang.Class.forName(db2_driver_name)
    
    db2_conn = jvm.java.sql.DriverManager.getConnection(db2_url_ssl, db2_username, db2_password)
    db2_conn.setAutoCommit(False)
    db2_statement = db2_conn.createStatement()
    
except Exception as err:
    traceback.print_exc()
    raise    

#############

rs = db2_statement.executeQuery("SELECT COUNT(*) FROM prodzu.WVITRSTH")

if rs.next():
    count = rs.getInt(1)  
    print("Total records in WVITRSTH:", count)
else:
    print("No result returned.")
    
########

%connections postgres_ross
%connections Q10_Connection
%connections DI5G_Ross


%%configure
{
  "--extra-jars": "s3://ross-library-files/db2_drivers/db2jcc4.jar,s3://ross-library-files/db2_drivers/db2jcc_license_cu-11.5.7.fp0.jar,s3://ross-library-files/db2_drivers/db2jcc_license_cisuz-jcc_v11_5_7_fp0.jar"
}


df = spark.read.format("jdbc") \
    .option("url", db2_url) \
    .option("driver", db2_driver_name) \
    .option("query", d2_query) \
    .option("user", db2_username) \
    .option("password", db2_password) \
    .option("fetchsize","25000") \
    .option("numPartitions","20") \
    .option("sslConnection", "true") \
    .load()

df.show(2)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>>  CURRENT_DATE, CURRENT_TIMESTAMP, CURRENT DATE, CURRENT TIMESTAMP
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql.functions import current_date, current_timestamp

df = df.withColumn("current_date_col", current_date())       
df = df.withColumn("current_timestamp_col", current_timestamp())  
  
  
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>>  MAX, RECORD COUNT, COUNT, RECORD
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   
sql_query = """
    SELECT MAX(MR_TASK) AS max_mr_task
    FROM ROSS.ross_mrt_task
"""

df_mr_task = spark.read.format("jdbc") \
    .option("url", postgres_jdbc_url) \
    .option("query", sql_query) \
    .option("user", postgres_user) \
    .option("password", postgres_password) \
    .load()

result = df_mr_task.collect()[0]["max_mr_task"]

print(result)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>>  NULL ISSUE, NULL, HANDLE NULL
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 
def sql_str(val):
    if val is None or str(val).strip() == "":
        return "NULL"
    return f"'{val}'"

def sql_date(val):
    if val is None or str(val).strip() == "":
        return "NULL"
    return f"'{val}'"

def sql_num(val):
    if val is None or str(val).strip() == "":
        return "NULL"
    return str(val)

{sql_num(pv_my_task)},
{sql_str(pv_vin_first_part)},
{sql_date(pv_ro_open_date)},

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>>  ALREADY EXISTS, EXISTS, ALREADY, ANTI, LEFTANTI, KEY COLUMNS
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

existing_keys = spark.read.format("jdbc") \
    .option("url", postgres_jdbc_url) \
    .option("dbtable", "(SELECT vin_first_part, chassis_11_17, ag_dlr_code, defect_code, repair_order_nbr FROM ROSS.ross_mrt_task) as t") \
    .option("user", postgres_user) \
    .option("password", postgres_password) \
    .load()

df_final_to_insert = df_load_dataset_unique.join(
    existing_keys,
    on=["vin_first_part", "chassis_11_17", "ag_dlr_code", "defect_code", "repair_order_nbr"],
    how="left_anti"
)

print(f"Final insertable rows: {df_final_to_insert.count()}")

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>>  DROPDUPLICATES, DROP DUPLICATES, DROP, DUPLICATES
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

df_to_insert = df_with_task.dropDuplicates(
    ["vin_first_part","chassis_11_17","ag_dlr_code","defect_code","repair_order_nbr"]
)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>>  SELECT WITH CAST, SELECT, CAST
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql.functions import col

df_load_dataset = df_with_task.select(
    col("MR_TASK").cast("int").alias("MR_TASK"),
    col("VIN_FIRST_PART"),
    col("AG_DLR_CODE").cast("int").alias("AG_DLR_CODE"),
    col("REPAIR_ORDER_NBR"),
    col("RO_OPEN_DATE").cast("date").alias("RO_OPEN_DATE"),
)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>>  MONOTONICALLY_INCREASING_ID, INCREMENT, INCREMENTAL, ROWNUM, NEXT
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql.functions import col, monotonically_increasing_id, lit, current_date

# 1. Get max MR_TASK
sql_query = """
    SELECT MAX(MR_TASK) AS max_mr_task
    FROM ROSS.ross_mrt_task
"""
df_mr_task = spark.read.format("jdbc") \
    .option("url", postgres_jdbc_url) \
    .option("query", sql_query) \
    .option("user", postgres_user) \
    .option("password", postgres_password) \
    .load()

my_task_init = df_mr_task.collect()[0][0] or 0

df_insert = (
    df_source
    .withColumn("mr_task", (lit(my_task_init) + monotonically_increasing_id() + 1).cast("int"))
    .withColumn("ag_dlr_code", col("AG_DLR_CODE").cast("int"))   
    .withColumn("emp_id", col("EMP_ID").cast("int"))             
    .withColumn("task_end_date", current_date())                  
    .withColumn("task_create_date", col("TASK_CREATE_DATE").cast("timestamp"))
    .withColumn("ro_open_date", col("RO_OPEN_DATE").cast("timestamp"))
    .withColumn("ro_close_date", col("RO_CLOSE_DATE").cast("timestamp"))
    .withColumn("recall_start_date", col("RECALL_START_DATE").cast("timestamp"))
    .withColumn("camp_strt_dt", col("CAMP_STRT_DT").cast("timestamp"))
    .withColumn("ssf_start_date", col("SSF_START_DATE").cast("timestamp"))
    .withColumn("rmdy_avl_date", col("RMDY_AVL_DATE").cast("timestamp"))
    .select(
        "mr_task","VIN_FIRST_PART","CHASSIS_11_17","ag_dlr_code","DEFECT_CODE",
        "REPAIR_ORDER_NBR","MR_SOURCE","ro_open_date","ro_close_date",
        "task_create_date","recall_start_date","camp_strt_dt",
        "ssf_start_date","rmdy_avl_date","task_end_date",
        "STATUS_MASTER_ID","AG_DLR_REGION","AG_DLR_MARKET","emp_id"
    )
)

(
    df_insert.write.format("jdbc")
    .option("url", postgres_jdbc_url)
    .option("dbtable", "ROSS.ross_mrt_task")
    .option("user", postgres_user)
    .option("password", postgres_password)
    .mode("append")
    .save()
)

---

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

windowSpec = Window.orderBy(lit(1))  # dummy order, just to enumerate rows

df_with_task = df_insert.withColumn(
    "MR_TASK",
    row_number().over(windowSpec) + my_task_init
)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>>  OPTIMIZE, OPTIMIZATION, FETCHSIZE, PARTITIONCOLUMN, LOWERBOUND, UPPERBOUND
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

df_DCS = spark.read.format("jdbc") \
    .option("url", dcs_oracle_url) \
    .option("driver", dcs_oracle_driver) \
    .option("dbtable", "DCS.REPAIRORDER") \
    .option("user", dcs_oracle_user) \
    .option("password", dcs_oracle_password) \
    .option("fetchsize", 25000) \
    .option("numPartitions", 10) \
    .option("partitionColumn", "RO_OPEN_DATE") \
    .option("lowerBound", "TO_DATE(SYSDATE-7,'YYYY-MM-DD')") \
    .option("upperBound", "TO_DATE(SYSDATE-1,'YYYY-MM-DD')") \
    .load()

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> DATE VALIDATION, TS TO DATE, TIMESTAMP TO DATE, DATE CONVERSION
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

SELECT *
FROM ross.ross_unreachable_vehicle_status
WHERE DATE(last_update_ts) = '2025-09-17';

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> BATCH LOAD, OPTIMIZATION, OPTIMIZE, ITERATOR
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

###########################################################################
# RUNNING BUT NOT DB UPDATE
###########################################################################

batch_size = 5000
batch_counter = 0

# Iterate over rows using toLocalIterator() to avoid collecting all rows
for row in df_vehicle_exclude_list_single.toLocalIterator():
    batch_counter += 1

    lv_product_type    = row["product_type"]
    lv_vin_first_part  = row["vin_first_part"]
    lv_chassis_nbr     = row["chassis_nbr"]
    lv_source_system   = row["source_system"]
    lv_process_status  = row["process_status"]
    lv_vehicle_status  = row["vehicle_status"]
    lv_task_id         = row["task_id"]

    # Map vehicle status
    if lv_vehicle_status == "D":
        lv_vehicle_status_upd = '005'
    elif lv_vehicle_status == "S":
        lv_vehicle_status_upd = '007'
    else:
        lv_vehicle_status_upd = '010'

    lv_ownership_status_code = '010'
    lv_additional_notes      = 'This has been updated as part of exclude vin'
    db2_last_update_by       = 'PRYUWXVN'
    pg_last_update_by        = 'GLXLDVIN'

    ###########################################################################################################
    # Postgres UPSERT using ON CONFLICT
    ###########################################################################################################
    upsert_sql_pg = f"""
        INSERT INTO ross.ross_unreachable_vehicle_status (
            vin_first_part, chassis_11_17, vehicle_status_code,
            ownership_status_code, additional_notes, last_update_by, last_update_ts
        )
        VALUES (
            '{lv_vin_first_part}', '{lv_chassis_nbr}', '{lv_vehicle_status_upd}',
            '{lv_ownership_status_code}', '{lv_additional_notes}', '{pg_last_update_by}', NOW()
        )
        ON CONFLICT (vin_first_part, chassis_11_17)
        DO UPDATE SET
            vehicle_status_code   = EXCLUDED.vehicle_status_code,
            ownership_status_code = EXCLUDED.ownership_status_code,
            additional_notes      = EXCLUDED.additional_notes,
            last_update_by        = EXCLUDED.last_update_by,
            last_update_ts        = NOW();
    """
    pg_statement.addBatch(upsert_sql_pg)

    ###########################################################################################################
    # DB2 MERGE
    ###########################################################################################################
    merge_sql_db2 = """
    MERGE INTO prodzu.wvitvsos AS t
    USING (VALUES (?, ?, ?, ?, ?, ?))
          AS s(vin_first_part, chassis_11_17, vehicle_status_code,
               ownership_status_code, additional_notes, last_update_by)
    ON (t.vin_first_part = s.vin_first_part AND t.chassis_11_17 = s.chassis_11_17)
    WHEN MATCHED THEN
        UPDATE SET
            vehicle_status_code   = s.vehicle_status_code,
            ownership_status_code = s.ownership_status_code,
            additional_notes      = s.additional_notes,
            last_update_by        = s.last_update_by,
            last_update_ts        = CURRENT TIMESTAMP
    WHEN NOT MATCHED THEN
        INSERT (vin_first_part, chassis_11_17, vehicle_status_code,
                ownership_status_code, additional_notes, last_update_by, last_update_ts)
        VALUES (s.vin_first_part, s.chassis_11_17, s.vehicle_status_code,
                s.ownership_status_code, s.additional_notes, s.last_update_by, CURRENT TIMESTAMP)
    """
    
    ps = db2_conn.prepareStatement(merge_sql_db2)
    ps.setString(1, lv_vin_first_part)
    ps.setString(2, lv_chassis_nbr)
    ps.setString(3, lv_vehicle_status_upd)
    ps.setString(4, lv_ownership_status_code)
    ps.setString(5, lv_additional_notes)
    ps.setString(6, db2_last_update_by)
    ps.addBatch()

    ###########################################################################################################
    # Update Exclude List
    ###########################################################################################################
    update_sql = f"""
        UPDATE ross.ross_vehicle_exclude_list
        SET process_status = 'C'
        WHERE product_type = '1'
          AND vin_first_part = '{lv_vin_first_part}'
          AND chassis_nbr    = '{lv_chassis_nbr}'
    """
    pg_statement.addBatch(update_sql)

    ###########################################################################################################
    # Commit in batches
    ###########################################################################################################
    if batch_counter == batch_size:
        pg_statement.executeBatch()
        db2_statement.executeBatch()
        pg_conn.commit()
        db2_conn.commit()
        print(f"Committed {batch_size} records.")
        batch_counter = 0

# Final commit for remaining rows
pg_statement.executeBatch()
db2_statement.executeBatch()
pg_conn.commit()
db2_conn.commit()

print("Database Updated Successfully.")

# Close connections
pg_statement.close()
pg_conn.close()
db2_statement.close()
db2_conn.close()

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> LOOP, COLLECT, ITERATOR
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

###########################################################
# 4 - LOAD INTO ROSS_UNREACHABLE_VEHICLE_STATUS
###########################################################
process_counter = 4

try:
     
    batch_size = 50000
    batch_counter = 0

    for row in df_vehicle_exclude_list.collect():
        batch_counter = batch_counter + 1
        
        lv_product_type    = row["product_type"]
        lv_vin_first_part  = row["vin_first_part"]
        lv_chassis_nbr     = row["chassis_nbr"]
        lv_source_system   = row["source_system"]
        lv_process_status  = row["process_status"]
        lv_vehicle_status  = row["vehicle_status"]
        lv_task_id         = row["task_id"]
    
        if lv_vehicle_status == "D":
            lv_vehicle_status_upd = '005'
        elif lv_vehicle_status == "S":
            lv_vehicle_status_upd = '007'
        else:
            lv_vehicle_status_upd = '010'
    
        lv_ownership_status_code = '010'
        lv_additional_notes      = 'This has been updated as part of exclude vin'
        db2_last_update_by       =  'PRYUWXVN'
        pg_last_update_by        =  'GLXLDVIN'
        
        ###########################################################################################################
        ### POSTGRES EXECUTION 
        ###########################################################################################################

        check_sql = f"""
            SELECT vehicle_status_code,
                   ownership_status_code,
                   additional_notes,
                   last_update_by
            FROM ross.ross_unreachable_vehicle_status
            WHERE vin_first_part = '{lv_vin_first_part}' 
              AND chassis_11_17  = '{lv_chassis_nbr}'
        """
        rs = pg_statement.executeQuery(check_sql)
    
        if rs.next():  
            db_vehicle_status   = rs.getString("vehicle_status_code")
            db_ownership_status = rs.getString("ownership_status_code")
            db_notes            = rs.getString("additional_notes")
            db_last_update_by   = rs.getString("last_update_by")
            
            #######################################
            #######################################
            
            if (db_vehicle_status   != lv_vehicle_status_upd     or
                db_ownership_status != lv_ownership_status_code  or
                db_notes            != lv_additional_notes       or
                db_last_update_by   != pg_last_update_by):
                
                update_sql = f"""
                    UPDATE ross.ross_unreachable_vehicle_status
                    SET 
                        vehicle_status_code   = '{lv_vehicle_status_upd}',
                        ownership_status_code = '{lv_ownership_status_code}',
                        additional_notes      = '{lv_additional_notes}',
                        last_update_by        = '{pg_last_update_by}',
                        last_update_ts        = NOW()
                    WHERE chassis_11_17       = '{lv_chassis_nbr}'
                      AND vin_first_part      = '{lv_vin_first_part}'
                """
                pg_statement.executeUpdate(update_sql)
                
            #######################################
            #######################################
                        
        else: 
            insert_sql = f"""
                INSERT INTO ross.ross_unreachable_vehicle_status (
                    vin_first_part, 
                    chassis_11_17, 
                    vehicle_status_code,
                    ownership_status_code, 
                    additional_notes, 
                    last_update_by, 
                    last_update_ts
                ) VALUES (
                    '{lv_vin_first_part}', 
                    '{lv_chassis_nbr}', 
                    '{lv_vehicle_status_upd}',
                    '{lv_ownership_status_code}', 
                    '{lv_additional_notes}', 
                    '{pg_last_update_by}', 
                     NOW()
                )
            """
            pg_statement.executeUpdate(insert_sql)

        ###########################################################################################################
        ### DB2 EXECUTION 
        ###########################################################################################################
        
        check_sql = f"""
            SELECT vehicle_status_code,
                   ownership_status_code,
                   additional_notes,
                   last_update_by
            FROM prodzu.wvitvsos
            WHERE vin_first_part = '{lv_vin_first_part}' 
              AND chassis_11_17  = '{lv_chassis_nbr}'
        """
        rs = db2_statement.executeQuery(check_sql)
    
        if rs.next():  
            db_vehicle_status   = rs.getString("vehicle_status_code")
            db_ownership_status = rs.getString("ownership_status_code")
            db_notes            = rs.getString("additional_notes")
            db_last_update_by   = rs.getString("last_update_by")
            
            #######################################
            #######################################
            
            if (db_vehicle_status   != lv_vehicle_status_upd     or
                db_ownership_status != lv_ownership_status_code  or
                db_notes            != lv_additional_notes       or
                db_last_update_by   != db2_last_update_by):
                
                update_sql = f"""
                    UPDATE prodzu.wvitvsos
                    SET 
                        vehicle_status_code   = '{lv_vehicle_status_upd}',
                        ownership_status_code = '{lv_ownership_status_code}',
                        additional_notes      = '{lv_additional_notes}',
                        last_update_by        = '{db2_last_update_by}',
                        last_update_ts        =  CURRENT TIMESTAMP
                    WHERE chassis_11_17       = '{lv_chassis_nbr}'
                      AND vin_first_part      = '{lv_vin_first_part}'
                """
                db2_statement.executeUpdate(update_sql)
                
            #######################################
            #######################################
                        
        else: 
            insert_sql = f"""
                INSERT INTO prodzu.wvitvsos (
                    vin_first_part, 
                    chassis_11_17, 
                    vehicle_status_code,
                    ownership_status_code, 
                    additional_notes, 
                    last_update_by, 
                    last_update_ts
                ) VALUES (
                    '{lv_vin_first_part}', 
                    '{lv_chassis_nbr}', 
                    '{lv_vehicle_status_upd}',
                    '{lv_ownership_status_code}', 
                    '{lv_additional_notes}', 
                    '{db2_last_update_by}', 
                     CURRENT TIMESTAMP
                )
            """
            db2_statement.executeUpdate(insert_sql)
            
        ###########################################################################################################
        ### UPDATE ROSS_VEHICLE_EXCLUDE_LIST
        ###########################################################################################################
        
        update_sql = f"""
            UPDATE ross.ross_vehicle_exclude_list
            SET process_status = 'C'
            WHERE product_type = '1'
              AND vin_first_part = '{lv_vin_first_part}'
              AND chassis_nbr    = '{lv_chassis_nbr}'
        """
        pg_statement.executeUpdate(update_sql)

        if batch_counter == batch_size:
            pg_conn.commit()
            db2_conn.commit()
            
            print(f"Committed {batch_size} records.")
            batch_counter = 0  
            
        ###########################################################################################################
        ### END LOOP PROCESS
        ###########################################################################################################
        
    print(f"Database Updated Successfully.")
    
    pg_conn.commit()
    db2_conn.commit()

    pg_statement.close()
    pg_conn.close()
    
    db2_statement.close()
    db2_conn.close()
    
    print(f"{success_status} : {ts} : [ {process_counter} ] : LOAD INTO ROSS_UNREACHABLE_VEHICLE_STATUS")

except Exception as err:
    traceback.print_exc()
    raise

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> LOOP, COLLECT, BATCH LOAD, LOAD, ITERATOR, ITERATE
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

try:

    import time
    start_time = time.time()   
    
    ##########################################################################
    # Batch Size Definiation
    ##########################################################################
    
    batch_size = 5000
    batch_counter = 0
    
    ##########################################################################
    # Prepare Postgres Statement (JDBC)
    ##########################################################################
    
    pg_statement = pg_conn.createStatement()
    
    ##########################################################################
    # Prepare DB2 MERGE PreparedStatement (JDBC)
    ##########################################################################
    
    merge_sql_db2 = """
    MERGE INTO prodzu.wvitvsos AS t
    USING (VALUES (?, ?, ?, ?, ?, ?))
          AS s(vin_first_part, chassis_11_17, vehicle_status_code,
               ownership_status_code, additional_notes, last_update_by)
    ON (t.vin_first_part = s.vin_first_part AND t.chassis_11_17 = s.chassis_11_17)
    WHEN MATCHED THEN
        UPDATE SET
            vehicle_status_code   = s.vehicle_status_code,
            ownership_status_code = s.ownership_status_code,
            additional_notes      = s.additional_notes,
            last_update_by        = s.last_update_by,
            last_update_ts        = CURRENT TIMESTAMP
    WHEN NOT MATCHED THEN
        INSERT (vin_first_part, chassis_11_17, vehicle_status_code,
                ownership_status_code, additional_notes, last_update_by, last_update_ts)
        VALUES (s.vin_first_part, s.chassis_11_17, s.vehicle_status_code,
                s.ownership_status_code, s.additional_notes, s.last_update_by, CURRENT TIMESTAMP)
    """
    ps_db2 = db2_conn.prepareStatement(merge_sql_db2)
    
    #########################################################################
    # Loop through Spark DataFrame rows
    #########################################################################
    
    for row in df_vehicle_exclude_list.toLocalIterator():
        lv_product_type    = row["product_type"]
        lv_vin_first_part  = row["vin_first_part"]
        lv_chassis_nbr     = row["chassis_nbr"]
        lv_source_system   = row["source_system"]
        lv_process_status  = row["process_status"]
        lv_vehicle_status  = row["vehicle_status"]
        lv_task_id         = row["task_id"]
    
        # Map vehicle status
        if lv_vehicle_status == "D":
            lv_vehicle_status_upd = '005'
        elif lv_vehicle_status == "S":
            lv_vehicle_status_upd = '007'
        else:
            lv_vehicle_status_upd = '010'
    
        lv_ownership_status_code = '010'
        lv_additional_notes      = 'This has been updated as part of exclude vin'
        db2_last_update_by       = 'PRYUWXVN'
        pg_last_update_by        = 'GLXLDVIN'
    
        #########################################################################
        # Postgres UPSERT 
        #########################################################################
        
        upsert_sql_pg = f"""
        INSERT INTO ross.ross_unreachable_vehicle_status (
            vin_first_part, chassis_11_17, vehicle_status_code,
            ownership_status_code, additional_notes, last_update_by, last_update_ts
        ) VALUES (
            '{lv_vin_first_part}', '{lv_chassis_nbr}', '{lv_vehicle_status_upd}',
            '{lv_ownership_status_code}', '{lv_additional_notes}', '{pg_last_update_by}', CURRENT_TIMESTAMP
        )
        ON CONFLICT (vin_first_part, chassis_11_17)
        DO UPDATE SET
            vehicle_status_code   = EXCLUDED.vehicle_status_code,
            ownership_status_code = EXCLUDED.ownership_status_code,
            additional_notes      = EXCLUDED.additional_notes,
            last_update_by        = EXCLUDED.last_update_by,
            last_update_ts        = CURRENT_TIMESTAMP;
        """
        pg_statement.addBatch(upsert_sql_pg)
    
        #########################################################################
        # Postgres Exclude list update
        #########################################################################
        
        update_exclude_sql = f"""
        UPDATE ross.ross_vehicle_exclude_list
        SET process_status = 'C'
        WHERE product_type = '1'
          AND vin_first_part = '{lv_vin_first_part}'
          AND chassis_nbr    = '{lv_chassis_nbr}';
        """
        pg_statement.addBatch(update_exclude_sql)
    
        #########################################################################
        # DB2 MERGE batch
        #########################################################################
        
        ps_db2.setString(1, lv_vin_first_part)
        ps_db2.setString(2, lv_chassis_nbr)
        ps_db2.setString(3, lv_vehicle_status_upd)
        ps_db2.setString(4, lv_ownership_status_code)
        ps_db2.setString(5, lv_additional_notes)
        ps_db2.setString(6, db2_last_update_by)
        ps_db2.addBatch()
    
        batch_counter += 1
    
        #########################################################################
        # Execute and commit batches
        #########################################################################
        
        if batch_counter == batch_size:
            # Postgres
            pg_statement.executeBatch()
            pg_conn.commit()
    
            # DB2
            ps_db2.executeBatch()
            db2_conn.commit()
    
            print(f"Committed {batch_size} records.")
            batch_counter = 0
    
    #########################################################################
    # Final commit for remaining rows
    #########################################################################
    
    if batch_counter > 0:
        pg_statement.executeBatch()
        pg_conn.commit()
    
        ps_db2.executeBatch()
        db2_conn.commit()
    
    print("Database Updated Successfully.")
    
    #########################################################################
    # Close connections
    #########################################################################
    
    pg_statement.close()
    pg_conn.close()
    ps_db2.close()
    db2_conn.close()
    
    #########################################################################
    # Validate Teime Consumed
    #########################################################################
    
    end_time = time.time()  
    elapsed_time = end_time - start_time
    print(f"Query execution time: {elapsed_time:.2f} seconds")
    
    ##########################################################################
    # Process Completion Status
    ##########################################################################
    
    print(f"vehicle_exclude is completed Successfully.")

except Exception as err:
    traceback.print_exc()
    raise
    
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> AWS GLUE, GLUE, GLU, CONFIG, CONFIGURATION
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

%help
%connections 
%idle_timeout 2880
%glue_version 5.0
%worker_type G.1X
%number_of_workers 5   

---
# Create a DynamicFrame from a table in the AWS Glue Data Catalog and display its schema
dyf = glueContext.create_dynamic_frame.from_catalog(database='database_name', table_name='table_name')
dyf.printSchema()

# Convert the DynamicFrame to a Spark DataFrame and display a sample of the data
df = dyf.toDF()
df.show()

# Write the data in the DynamicFrame to a location in Amazon S3 and a table for it in the AWS Glue Data Catalog
s3output = glueContext.getSink(
  path="s3://bucket_name/folder_name",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase="demo", catalogTableName="populations"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(DyF)

    
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> PARTATION, PARTATION COUNT, RDD, REPARTITION, COALESCE
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# Suppose df is your PySpark DataFrame
num_partitions = df.rdd.getNumPartitions()

# Repartition to 10 partitions
df_repart = df.repartition(10)
num_partitions = df_repart.rdd.getNumPartitions()

# Or reduce partitions
df_coalesced = df.coalesce(5)
num_partitions = df_coalesced.rdd.getNumPartitions()

def count_in_partition(iterator):
    yield sum(1 for _ in iterator)

partition_counts = df.rdd.mapPartitions(count_in_partition).collect()

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> PYSPARK VS PYTHON, DATA VALIDATION, VALIDATION, VALIDATE, ISNULL, NONE
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

## pyspark columns Validation
from pyspark.sql.functions import col
df_filtered = df.filter(col("reserve_date").isNull())
df_filtered = df.filter(col("reserve_date").isNotNull())

## Python Validation
if reserve_date is None:
    reserve_date = "NULL"

# Or also check empty string
if reserve_date is None or reserve_date.strip() == "":
    reserve_date = "NULL"

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> SKIP INSERT, UPDATE, DELETE STATUS, .EXECUTE()
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

_ = statement.execute(delete_sql)   # throw away the boolean
deleted_rows = statement.getUpdateCount()
print(f"Deleted Records from ... : {deleted_rows}")

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> DF2S3, WRITE DF, WRITE, WRITE TO S3
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

import boto3
from datetime import datetime

# Your DataFrame
# df = ...  # your existing DataFrame

# Config
bucket = "your-bucket-name"
base_path = "your/base/path"   # e.g. "reports" or "" for root

# Step 1: Create folder name with timestamp
now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
folder_name = f"defect_{now_str}"
temp_prefix = f"{base_path}/{folder_name}/temp_output/"
final_prefix = f"{base_path}/{folder_name}/"
final_key = f"{final_prefix}abc.csv"

# Step 2: Write DataFrame as single CSV to temp folder
temp_s3_path = f"s3://{bucket}/{temp_prefix}"
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_s3_path)

# Step 3: Rename part file to abc.csv using boto3
s3 = boto3.client("s3")
response = s3.list_objects_v2(Bucket=bucket, Prefix=temp_prefix)

# Get the first file key (assuming only one CSV file in temp folder)
file_key = response['Contents'][0]['Key']

s3.copy_object(
    Bucket=bucket,
    CopySource={"Bucket": bucket, "Key": file_key},
    Key=final_key
)

# Step 4: Delete temp folder files
for obj in response.get("Contents", []):
    s3.delete_object(Bucket=bucket, Key=obj["Key"])

print(f"DataFrame written as s3://{bucket}/{final_key}")


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> OPTIMIZATION, OPTIMIZE, PARTATION
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

bounds = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/mydb") \
    .option("query", "SELECT UNIX_TIMESTAMP(MIN(last_updated)) AS min_ts, \
                             UNIX_TIMESTAMP(MAX(last_updated)) AS max_ts \
                      FROM my_table") \
    .option("user", "root") \
    .option("password", "pass") \
    .load()

row = bounds.collect()[0]
lowerBound = row["min_ts"]
upperBound = row["max_ts"]

print("lowerBound:", lowerBound)
print("upperBound:", upperBound)

###############

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/mydb") \
    .option("query", """
        SELECT id, col1, col2, UNIX_TIMESTAMP(last_updated) AS last_updated_ts
        FROM my_table
    """) \
    .option("partitionColumn", "last_updated_ts") \
    .option("lowerBound", lowerBound) \
    .option("upperBound", upperBound) \
    .option("numPartitions", 12) \
    .load()

##############

SELECT *, MOD(ABS(CRC32(col1)), 12) AS bucket
FROM my_table

.option("partitionColumn", "bucket") \
.option("lowerBound", 0) \
.option("upperBound", 11) \
.option("numPartitions", 12)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> PYSPARK UPDATE VS JDBC UPDATE
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
>> COMMIT, STATEMENT.EXECUTE("COMMIT"), CON.COMMIT
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        
update_query = f"""
UPDATE ROSS.ross_vehicle_defects V
    SET 
    SSF_START_DATE     = NULL,
    LAST_UPDT_PROCESS  = 'GL-VD',
    LAST_UPDT_TSTMP    = CURRENT_TIMESTAMP
WHERE V.SSF_START_DATE IS NOT NULL
AND EXISTS
(
    SELECT 1
    FROM ROSS.ross_defects D
    WHERE D.PRODUCT_TYPE   = V.PRODUCT_TYPE
      AND D.DEFECT_CODE    = V.DEFECT_CODE
      AND D.STOP_SALE_FLAG = 'N'
      AND SUBSTR(D.DEFECT_CODE, 1, 4) <> 'STOP'
);  
"""    
statement.execute(update_query)
updated_rows = statement.getUpdateCount()    
statement.execute("commit")
    
#####

statement.execute(update_query)
updated_rows = statement.getUpdateCount()  
con.commit
