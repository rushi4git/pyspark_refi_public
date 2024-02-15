from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator
from datetime import datetime, timedelta, date
import boto3 , botocore
from airflow.models import Variable
from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
from pyspark.sql.functions import col, udf
# from pyspark.sql.types import *
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType, TimestampType, DecimalType, BooleanType
from pyspark.sql import Window
from pyspark.sql.functions import row_number, lit, get_json_object, from_utc_timestamp, when, coalesce #, to_date, current_date , substring
from pyspark.sql.functions import sha2, concat_ws, concat
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql import functions as F
# import pandas as pd
import json
from pyspark.sql.functions import monotonically_increasing_id, col, expr

default_args = {
    'owner': 'raj',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'account_portfolio_refi',
    default_args=default_args,
    schedule_interval=None,
)
current_date1 = datetime.now().strftime("%Y_%m_%d")
print(current_date1)

today_file='refi_'+str(current_date1)
current_date2 = datetime.now()
yesterday = current_date2 - timedelta(days=1)

previous_day = yesterday.strftime("%Y_%m_%d")
print(previous_day)
yesterday_file = 'refi_'+str(previous_day)

key_id = Variable.get("AWS_ACCESS_KEY_ID")
secret_key_id = Variable.get("AWS_SECRET_ACCESS_KEY")
bucket_name="training-pyspark"
old_name=Variable.get("source_field_names")
target_name=Variable.get("target_field_names")
key=f"drop/{today_file}.parquet"
key2 = f"drop/{yesterday_file}.parquet"


def check_file_in_s3():
    
    s3= boto3.resource('s3',aws_access_key_id=key_id, aws_secret_access_key=secret_key_id)
    try:
        s3.Object(bucket_name,key).load()
        print(key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":#404 file does not found error
            # print("file does not exist")
            return False
        else :
            return f"error : {e}"
        
# Create Spark session
'''
spark = SparkSession.builder \
    .appName("account_portfolio_refi") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
    .getOrCreate()
'''
spark = SparkSession.builder \
    .appName("account_portfolio_refi") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.parquet.columnarReaderBatchSize", "2048") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.sources.bucketing.enabled", "false") \
    .getOrCreate()
# Configure AWS credentials in Spark session
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", key_id)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key_id)

def convert(string):
    column_name_list = list(string.split("\r\n"))
    return column_name_list

#compare schema

def compare_schema_func():
    print("before load")
    today_df = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    yesterday_df = spark.read.parquet(f"s3a://{bucket_name}/{key2}", header=True, inferSchema=True)
    today_schema = today_df.schema
    yetserday_schema = yesterday_df.schema

    if today_schema == yetserday_schema:
        print("The schemas are identical.")
        return True
    else:
        print("The schemas are different.")
        print("Schema for day1:")
        print(day1_schema)
        print("Schema for day2:")
        print(day2_schema)
        return False
    
def loan_debt_sale_transformation():
    
    df = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    print('conversion started')
    old_names=convert(old_name)
    target_names=convert(target_name)
    print(old_names)
    print(target_names)
    mapping = dict(zip(old_names,target_names))
    df1=df.select([col(c).alias(mapping.get(c, c)) for c in df.columns])
    
    
    df1 = df.filter(df.dark == False)  
    
    
    # Defining a window spec.
    window_spec = Window.partitionBy("loan_id").orderBy(df1.published_at.desc())

    # Adding a row number column to the df
    df_with_row_number = df1.withColumn("rn", row_number().over(window_spec))

    # Most recent record per loan_id
    df2 = df_with_row_number.filter("rn = 1")
    
    df3= df2.filter(df2.sale_info != '[]')
    

    # parse value from JSON : loan_id, loan_uuid

    json_schema = StructType([StructField("loan_id", StringType(), True)])
    df_parsed = df3.withColumn("loan_data", from_json(col("loan_data"), json_schema))
    df4 = df_parsed.withColumn("loan_id", df_parsed["loan_data.loan_id"])

    json_schema = StructType([StructField("loan_uuid", StringType(), True)])
    df_parsed = df3.withColumn("loan_data_parsed", from_json(col("loan_data"), json_schema))
    df5 = df_parsed.withColumn("loan_uuid", df_parsed["loan_data_parsed.loan_uuid"])


    # Explode column to one row per owner per loan
    json_schema = ArrayType(StringType())
    df5 = df5.withColumn("financial_owners_array", from_json("financial_owners", json_schema))
    df5 = df5.select("*", explode("financial_owners_array").alias("financial_owner"))
    df5.count()

    # Define the schema for the JSON data
    json_schema = StructType([StructField("uuid", StringType(), True)])
    df_parsed = df5.withColumn("sale_info", from_json(col("sale_info"), json_schema))
    df6 = df_parsed.withColumn("loan_debt_sale_uuid", df_parsed["sale_info"]["uuid"])

    # single value from sale_info

    json_schema = ArrayType(StructType([StructField("uuid", StringType(), True)]))
    df_parsed = df5.withColumn("sale_info_parsed", from_json(col("sale_info"), json_schema))
    df6 = df_parsed.withColumn("uuid", col("sale_info_parsed")[0]["uuid"])

    schema = ArrayType(StructType([
        StructField("uuid", StringType(), True),
        StructField("sale_date", TimestampType(), True),
        StructField("debt_buyer", StringType(), True),
        StructField("loan_task_id", StringType(), True),
        StructField("percentage_sold", DoubleType(), True),
        StructField("sale_amount", DoubleType(), True),
        StructField("sale_fees_amount", DoubleType(), True),
        StructField("sale_interest_amount", DoubleType(), True),
        StructField("sale_principal_amount", DoubleType(), True),
        StructField("debt_sale_file_id", StringType(), True),
        StructField("debt_sale_file_date", TimestampType(), True),
        StructField("debt_sale_file_uuid", StringType(), True),
        StructField("rebuy_date", TimestampType(), True),
        StructField("rebuy_amount", DoubleType(), True),
        StructField("rebuy_reason", StringType(), True),
        StructField("rebuy_file_uuid", StringType(), True),
        StructField("rebuy_transaction_timestamp", TimestampType(), True),
        StructField("sale_transaction_timestamp", TimestampType(), True),
    ]))


    df_parsed = df5.withColumn("sale_info_parsed", from_json(col("sale_info"), schema))

    # Select the extracted fields as new columns
    df6 = df_parsed.select(
        "*",
        col("sale_info_parsed.uuid").alias("loan_debt_sale_uuid"),
        col("sale_info_parsed.sale_date").alias("sale_date"),
        col("sale_info_parsed.debt_buyer").alias("debt_buyer_name"),
        col("sale_info_parsed.loan_task_id").alias("loan_task_id"),
        col("sale_info_parsed.percentage_sold").alias("percentage_sold"),
        col("sale_info_parsed.sale_amount").alias("sale_amount"),
        col("sale_info_parsed.sale_fees_amount").alias("sale_fees_amount"),
        col("sale_info_parsed.sale_interest_amount").alias("sale_interest_amount"),
        col("sale_info_parsed.sale_principal_amount").alias("sale_principal_amount"),
        col("sale_info_parsed.debt_sale_file_id").alias("debt_sale_file_id"),
        col("sale_info_parsed.debt_sale_file_date").alias("debt_sale_file_date"),
        col("sale_info_parsed.debt_sale_file_uuid").alias("debt_sale_file_uuid"),
        col("sale_info_parsed.rebuy_date").alias("rebuy_date"),
        col("sale_info_parsed.rebuy_amount").alias("rebuy_amount"),
        col("sale_info_parsed.rebuy_reason").alias("rebuy_reason"),
        col("sale_info_parsed.rebuy_file_uuid").alias("rebuy_file_uuid"),
        col("sale_info_parsed.rebuy_transaction_timestamp").alias("rebuy_time"),
        col("sale_info_parsed.sale_transaction_timestamp").alias("created_time")
    )


    df6 = df6.drop("sale_info_parsed")
    
    def extract_array_column(df, column_name):
        return df.withColumn(column_name, col(column_name).getItem(0))


    columns_to_extract = ["created_time", "sale_date", "debt_buyer_name",
                        "loan_task_id", "percentage_sold", "sale_amount", "sale_fees_amount",
                        "sale_interest_amount", "sale_principal_amount", "debt_sale_file_id",
                        "debt_sale_file_date", "debt_sale_file_uuid", "rebuy_date", "rebuy_amount",
                        "rebuy_reason", "rebuy_file_uuid", "rebuy_time", "loan_debt_sale_uuid"]  # Add all column names you want to process

    df7 = df6
    for column in columns_to_extract:
        df7 = extract_array_column(df7, column)

    # Target Fields
    selected_columns = ["loan_id", "loan_uuid", "created_time", "sale_date", "debt_buyer_name",
                        "loan_task_id", "percentage_sold", "sale_amount", "sale_fees_amount",
                        "sale_interest_amount", "sale_principal_amount", "debt_sale_file_id",
                        "debt_sale_file_date", "debt_sale_file_uuid", "rebuy_date", "rebuy_amount",
                        "rebuy_reason", "rebuy_file_uuid", "rebuy_time", "loan_debt_sale_uuid"]
    
    loan_debt_sale = df7.select(selected_columns)
    print("Loan_debt_sale_transformation succeeded")
    
    
    target = loan_debt_sale.withColumn('extract_timestamp', lit(datetime.now()))

    #target.write.mode('overwrite').option("header", "true").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/loan_debt_sale_transform.parquet")

    target.coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/loan_debt_sale_transformation.parquet")

def loan_servicing_settlement_transformation():
    
    df = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    print('conversion started')
    old_names=convert(old_name)
    target_names=convert(target_name)
    print(old_names)
    print(target_names)
    mapping = dict(zip(old_names,target_names))
    df1=df.select([col(c).alias(mapping.get(c, c)) for c in df.columns])
    
    
    df1 = df.filter(df.dark == False) 
    
     # Defining a window spec.
    window_spec = Window.partitionBy("loan_id").orderBy(df1.published_at.desc())

    # Adding a row number column to the df
    df_with_row_number = df1.withColumn("rn", row_number().over(window_spec))

    # Most recent record per loan_id
    df2 = df_with_row_number.filter("rn = 1")
    
    lss_df3 = df2.filter(df2.settlements != '[]')
    # print("DataFrame after filtering:")
    

    # parse value from JSON : loan_id, loan_uuid

    json_schema = StructType([StructField("loan_id", StringType(), True)])
    df_parsed = lss_df3.withColumn("loan_data_parsed", from_json(col("loan_data"), json_schema))
    lss_df4 = df_parsed.withColumn("loan_id", df_parsed["loan_data_parsed.loan_id"])

    json_schema = StructType([StructField("loan_uuid", StringType(), True)])
    df_parsed = lss_df3.withColumn("loan_data_parsed", from_json(col("loan_data"), json_schema))
    lss_df5 = df_parsed.withColumn("loan_uuid", df_parsed["loan_data_parsed.loan_uuid"])

    #Explode column to one row per owner per loan
    json_schema = ArrayType(StringType())
    lss_df5 = lss_df5.withColumn("financial_owners_array", from_json("financial_owners", json_schema))
    lss_df5 = lss_df5.select("*", explode("financial_owners_array").alias("financial_owner"))

    schema = ArrayType(StructType([
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("end_date", TimestampType(), True),
        StructField("start_date", TimestampType(), True),
        StructField("payment_method", StringType(), True),
        StructField("settlement_type", StringType(), True),
        StructField("number_of_payments", DoubleType(), True),
        StructField("transaction_timestamp", TimestampType(), True),
    ]))


    df_parsed = lss_df5.withColumn("settlements_parsed", from_json(col("settlements"), schema))

    # Select the extracted fields as new columns
    lss_df6 = df_parsed.select(
        "*",
        col("settlements_parsed.amount").alias("total_amount"),
        col("settlements_parsed.status").alias("status"),
        col("settlements_parsed.end_date").alias("end_date"),
        col("settlements_parsed.start_date").alias("start_date"),
        col("settlements_parsed.payment_method").alias("payment_method"),
        col("settlements_parsed.settlement_type").alias("structure"),
        col("settlements_parsed.number_of_payments").alias("payment_count"),
        col("settlements_parsed.transaction_timestamp").alias("created_time")
    )


    lss_df6 = lss_df6.drop("sale_info_parsed")
    

    def extract_array_column(df, column_name):
        return df.withColumn(column_name, col(column_name).getItem(0))


    columns_to_extract = ["total_amount", "status", "end_date", "start_date", "payment_method",
                          "structure", "payment_count", "created_time"
                          ]

    lss_df7 = lss_df6
    for column in columns_to_extract:
        lss_df7 = extract_array_column(lss_df7, column)

    # hash(loan_id + start_date)
    lss_df8 = lss_df7.withColumn("loan_servicing_settlement_uuid", sha2(concat_ws("", lss_df7["loan_id"], lss_df7["start_date"]), 256))


    # Selecting only the targrted columns
    selected_columns = ["loan_id", "loan_uuid", "total_amount", "status", "end_date", "start_date", "payment_method",
                          "structure", "payment_count", "created_time" , "loan_servicing_settlement_uuid"]
    loan_servicing_settlement_df = lss_df8.select(selected_columns)
    print("loan_servicing_settlement_transformation succeeded")
    
    
    target = loan_servicing_settlement_df.withColumn('extract_timestamp', lit(datetime.now()))

    #target.write.mode('overwrite').option("header", "true").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/loan_servicing_settlement_transform.parquet")

    target.coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/loan_servicing_settlement_transformation.parquet")
    
def active_installment_transformation():
    
    df = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    print('conversion started')
    old_names=convert(old_name)
    target_names=convert(target_name)
    print(old_names)
    print(target_names)
    mapping = dict(zip(old_names,target_names))
    df1=df.select([col(c).alias(mapping.get(c, c)) for c in df.columns])
    
    
    df1 = df.filter(df.dark == False)
      
     # Defining a window spec.
    window_spec = Window.partitionBy("loan_id").orderBy(df1.published_at.desc())

    # Adding a row number column to the df
    df_with_row_number = df1.withColumn("rn", row_number().over(window_spec))

    # Most recent record per loan_id
    df2 = df_with_row_number.filter("rn = 1")
    
    ai_df3 = df2.filter(df2.installments != '[]')
    

    # parse value from JSON : loan_id, loan_uuid

    json_schema = StructType([StructField("loan_id", StringType(), True)])
    df_parsed = ai_df3.withColumn("loan_data_parsed", from_json(col("loan_data"), json_schema))
    ai_df4 = df_parsed.withColumn("loan_id", df_parsed["loan_data_parsed.loan_id"])

    json_schema = StructType([StructField("loan_uuid", StringType(), True)])
    df_parsed = ai_df3.withColumn("loan_data_parsed", from_json(col("loan_data"), json_schema))
    ai_df5 = df_parsed.withColumn("loan_uuid", df_parsed["loan_data_parsed.loan_uuid"])

    # Explode column to one row per owner per loan
    json_schema = ArrayType(StringType())
    ai_df5 = ai_df5.withColumn("installments_array", from_json("installments", json_schema))
    ai_df5 = ai_df5.select("*", explode("installments_array").alias("installment1"))
    


    schema = ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("interest_amount", DoubleType(), True),
        StructField("principal_amount", DoubleType(), True),
        StructField("late_fees_amount", DoubleType(), True),
        StructField("adjusted_effective_date", TimestampType(), True),
        StructField("original_effective_date", TimestampType(), True),
    ]))


    df_parsed = ai_df5.withColumn("installments_parsed", from_json(col("installments"), schema))

    # Select the extracted fields as new columns
    ai_df6 = df_parsed.select(
        "*",
        col("installments_parsed.id").alias("active_installment_id"),
        col("installments_parsed.interest_amount").alias("interest_amount"),
        col("installments_parsed.principal_amount").alias("principal_amount"),
        col("installments_parsed.late_fees_amount").alias("late_fees_amount"),
        col("installments_parsed.adjusted_effective_date").alias("adjusted_effective_date"),
        col("installments_parsed.original_effective_date").alias("original_effective_date"),
    )


    ai_df6 = ai_df6.drop("installments_parsed")

    def extract_array_column(df, column_name):
        return df.withColumn(column_name, col(column_name).getItem(0))
    columns_to_extract = ["interest_amount", "principal_amount", "active_installment_id", "late_fees_amount", "adjusted_effective_date", "original_effective_date"
                          ]
    ai_df7 = ai_df6
    for column in columns_to_extract:
        ai_df7 = extract_array_column(ai_df7, column)

    ai_df8 = ai_df7.withColumn("active_installment_uuid", sha2(col("id"), 256))

    # Selecting only the targrted columns
    selected_columns = ["loan_id", "loan_uuid", "active_installment_id", "interest_amount", "principal_amount", "late_fees_amount", "adjusted_effective_date", "original_effective_date", "active_installment_uuid"]
    active_installment_df = ai_df8.select(selected_columns)
    print("active_installment_transformation succeeded")
    
    
    target = active_installment_df.withColumn('extract_timestamp', lit(datetime.now()))

    #target.write.mode('overwrite').option("header", "true").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/active_installment_transformation.parquet")
    
    target.coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/active_installment_transformation.parquet")
    
def bankruptcy_transformation():
    
    df = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    print('conversion started')
    old_names=convert(old_name)
    target_names=convert(target_name)
    print(old_names)
    print(target_names)
    mapping = dict(zip(old_names,target_names))
    df1=df.select([col(c).alias(mapping.get(c, c)) for c in df.columns])
    
    
    df1 = df.filter(df.dark == False)
    
    
    # Defining a window spec.
    window_spec = Window.partitionBy("loan_id").orderBy(df1.published_at.desc())

    # Adding a row number column to the df
    df_with_row_number = df1.withColumn("rn", row_number().over(window_spec))

    # Most recent record per loan_id
    df2 = df_with_row_number.filter("rn = 1")
    
    # Filter to remove any row where bankruptcy_timeline = '[]' and bankruptcy_claims = '[]'
    bkc_df3 = df2.filter((df2.bankruptcy_timeline != '[]') & (df2.bankruptcy_claims != '[]'))
    

    json_schema = StructType([StructField("loan_id", StringType(), True)])
    df_parsed = bkc_df3.withColumn("loan_data_parsed", from_json(col("loan_data"), json_schema))
    bkc_df4 = df_parsed.withColumn("loan_id", df_parsed["loan_data_parsed.loan_id"])

    json_schema = StructType([StructField("loan_uuid", StringType(), True)])
    df_parsed = bkc_df4.withColumn("loan_data_parsed", from_json(col("loan_data"), json_schema))
    bkc_df5 = df_parsed.withColumn("loan_uuid", df_parsed["loan_data_parsed.loan_uuid"])

    #  The following fields all come from the bankruptcy_timeline and bankruptcy_claims columns, which are arrays of further JSON. These arrays need to be exploded so that each entry forms one row in the final table.
    json_schema = ArrayType(StringType())
    bkc_df6 = bkc_df5.withColumn("bankruptcy_timeline_array", from_json("bankruptcy_timeline", json_schema))
    bkc_df6 = bkc_df6.select("*", explode("bankruptcy_timeline_array").alias("bankruptcy_timeline_id"))

    bkc_df7 = bkc_df6.withColumn("bankruptcy_claims_array", from_json("bankruptcy_claims", json_schema))
    bkc_df7 = bkc_df7.select("*", explode("bankruptcy_claims_array").alias("bankruptcy_claims_id"))


    

    def min_petitioned_date(event_history):
        try:
            events = json.loads(event_history)
            petitioned_dates = [event["date"] for event in events if "status" in event and event["status"] == "petitioned"]
            return min(petitioned_dates) if petitioned_dates else None
        except (json.JSONDecodeError, KeyError):
            return None

    # Define the UDF to be used in Spark DataFrame
    min_petitioned_date_udf = udf(min_petitioned_date, StringType())

    # # Apply the UDF to extract the minimum petitioned date
    bkc_df6 = bkc_df5.withColumn("min_petitioned_date", min_petitioned_date_udf(col("bankruptcy_timeline")))

    def min_discharged_date(event_history):
        try:
            events = json.loads(event_history)
            discharged_dates = [event["date"] for event in events if "status" in event and event["status"] == "discharged"]
            return min(discharged_dates) if discharged_dates else None
        except (json.JSONDecodeError, KeyError):
            return None

    # Define the UDF to be used in Spark DataFrame
    min_discharged_date_udf = udf(min_discharged_date, StringType())

    # Apply the UDF to extract the minimum discharged date
    bkc_df7 = bkc_df6.withColumn("min_discharged_date", min_discharged_date_udf(col("bankruptcy_timeline")))

    schema = StructType([
        StructField("event_history", ArrayType(StructType([
            StructField("date", StringType(), True),
            StructField("status", StringType(), True),
            StructField("chapter", StringType(), True),
            StructField("event_id", StringType(), True),
            StructField("removed_on", StringType(), True),
            StructField("case_number", StringType(), True),
            StructField("confirmed_at", StringType(), True),
            StructField("attorney_name", StringType(), True),
            StructField("law_firm_name", StringType(), True),
            StructField("event_created_at", StringType(), True),
            StructField("proof_of_claim_date", StringType(), True),
            StructField("attorney_phone_number", StringType(), True),
            StructField("attorney_email_address", StringType(), True),
            StructField("representation_confirmed", StringType(), True),
        ]), True)),
    ])

    bkc_df7 = bkc_df7.withColumn("bankruptcy_timeline", from_json("bankruptcy_timeline", schema))
    # Create a temporary unique identifier for each row
    bkc_df7 = bkc_df7.withColumn("temp_bk_num", monotonically_increasing_id())
    # Explode the bankruptcy_timeline array to have one row per sub-blob
    exploded_df = bkc_df7.select("temp_bk_num", col("bankruptcy_timeline.event_history").alias("event_history"))

    # Explode the event_history array to have one row per event
    exploded_df = exploded_df.select("temp_bk_num", col("event_history.date").alias("date"), col("event_history.status").alias("status"))

    # Add a row number within each partition to get the unique identifier
    exploded_df = exploded_df.withColumn("temp_bk_num", col("temp_bk_num") + row_number().over(Window.partitionBy("temp_bk_num").orderBy("date")) - 1)

    # Join the exploded DataFrame back to the original DataFrame
    bkc_df7 = bkc_df7.join(exploded_df, "temp_bk_num", "left")

    # hash(loan_id + 'timeline' + temp_bk_num)
    bkc_df8 = bkc_df7.withColumn("bankruptcy_uuid1", sha2(concat_ws("", col("loan_id"), col("temp_bk_num")), 256))

    # Starting with the output of step 4, filter to rows where bankruptcy_claims <> '[]' then explode bankruptcy_claims column to one row per bankruptcy per loan
    bkc_df9 = bkc_df8.filter((bkc_df8.bankruptcy_claims != '[]'))

    # Parse the JSON content into a struct type
    bkc_df10 = bkc_df9.withColumn("bankruptcy_claims_array1", from_json("bankruptcy_claims", json_schema))

    # Explode the array column
    bkc_df11 = bkc_df10.select("*", explode("bankruptcy_claims_array1").alias("bankruptcy_claims_id"))

    # Define the schema for the JSON column
    schema = ArrayType(StructType([
        StructField("status", StringType(), True),
        StructField("chapter", StringType(), True),
        StructField("effective_date", StringType(), True)
    ]))

    # Extract values from the JSON column
    bkc_df11 = bkc_df11.withColumn("parsed_claims", from_json(col("bankruptcy_claims"), schema))

    # Create new columns for status, chapter, and claimed_date
    bkc_df11 = bkc_df11 \
        .withColumn("status", col("parsed_claims.status")) \
        .withColumn("chapter", col("parsed_claims.chapter")) \
        .withColumn("claimed_date", col("parsed_claims.effective_date")) \
        # .drop("parsed_claims")


    bkc_df11 = bkc_df11.withColumn("petitioned_date", expr("cast(NULL as date)"))
    bkc_df12 = bkc_df11.withColumn("discharged_date", expr("cast(NULL as date)"))

    # hash(loan_id + claimned_date)
    bkc_df13 = bkc_df12.withColumn("bankruptcy_uuid2", sha2(concat_ws("", col("loan_id"), col("claimed_date")), 256))

    # Union the results of step 6 and step 8
    bkc_df14 = bkc_df13.withColumn("bankruptcy_uuid", concat(col("bankruptcy_uuid1"), col("bankruptcy_uuid2")))

    def extract_array_column(df, column_name):
        return df.withColumn(column_name, col(column_name).getItem(0))
    columns_to_extract = ["claimed_date", "chapter", "status"]
    bkc_df15 = bkc_df14
    for column in columns_to_extract:
        bkc_df15 = extract_array_column(bkc_df15, column)

    # Selecting only the targrted columns
    selected_columns = ["loan_id", "loan_uuid", "claimed_date", "petitioned_date", "discharged_date", "bankruptcy_uuid", "chapter", "status"]
    bankruptcy_df = bkc_df15.select(selected_columns)
    print("bankruptcy_transformation succeeded")
    
    
    target = bankruptcy_df.withColumn('extract_timestamp', lit(datetime.now()))

    #target.write.mode('overwrite').option("header", "true").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/bankruptcy_transformation.parquet")
    
    target.coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/bankruptcy_transformation.parquet")

def interest_rate_transformation():
    
    df = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    print('conversion started')
    old_names=convert(old_name)
    target_names=convert(target_name)
    print(old_names)
    print(target_names)
    mapping = dict(zip(old_names,target_names))
    df1=df.select([col(c).alias(mapping.get(c, c)) for c in df.columns])
    
    
    df1 = df.filter(df.dark == False)
        
     # Defining a window spec.
    window_spec = Window.partitionBy("loan_id").orderBy(df1.published_at.desc())

    # Adding a row number column to the df
    df_with_row_number = df1.withColumn("rn", row_number().over(window_spec))

    # Most recent record per loan_id
    df2 = df_with_row_number.filter("rn = 1")
    
    from os import truncate
    json_schema = StructType([StructField("loan_id", StringType(), True)])
    df_parsed = df2.withColumn("loan_data_parsed", from_json(col("loan_data"), json_schema))
    ir_df3 = df_parsed.withColumn("loan_id", df_parsed["loan_data_parsed.loan_id"])

    json_schema = StructType([StructField("loan_uuid", StringType(), True)])
    df_parsed = df1.withColumn("loan_data_parsed", from_json(col("loan_data"), json_schema))
    ir_df4 = df_parsed.withColumn("loan_uuid", df_parsed["loan_data_parsed.loan_uuid"])

    loan_data_schema = StructType([
        StructField("term", StringType(), True),
        StructField("loan_id", StringType(), True),
        StructField("interest_rate_schedule", ArrayType(StructType([
            StructField("interest_rate", StringType(), True),
            StructField("effective_date", StringType(), True)
        ]), True), True)
    ])
    df_parsed = ir_df4.select(from_json('loan_data', loan_data_schema).alias('loan_data_struct'))
    ir_df5 = df_parsed.select(
        col('loan_data_struct.term').alias('term'),
        col('loan_data_struct.loan_id').alias('loan_id'),
        explode('loan_data_struct.interest_rate_schedule').alias('interest_rate_schedule')
    )

    ir_df_combined = ir_df4.alias('df4').join(ir_df5.alias('df5'), col('df4.loan_id') == col('df5.loan_id'), 'left_outer')

    # Select all columns from ir_df4 and the new column interest_rate_schedule
    ir_df5 = ir_df_combined.select(
        col('df4.*'),
        col('df5.interest_rate_schedule').alias('interest_rate_schedule')
    )

    # Parse interest_rate_schedule for interest_rate

    json_schema = StructType([StructField("loan_id", StringType(), True)])
    df_parsed = ir_df3.withColumn("loan_data", from_json(col("loan_data"), json_schema))
    ir_df3 = df_parsed.withColumn("loan_id", df_parsed["loan_data.loan_id"])

    ir_df6 = ir_df5.withColumn("interest_rate", col("interest_rate_schedule.interest_rate"))
    ir_df7 = ir_df6.withColumn("effective_date", col("interest_rate_schedule.effective_date"))

    # "hash the 'id' column Until that field is available, hash (loan_id + interest_rate + effective_date) instead"

    ir_df8 = ir_df7.withColumn("interest_rate_uuid", sha2(concat_ws("", col("loan_id"), col("interest_rate"), col("effective_date")), 256))

    # rename
    ir_df9 = ir_df8.withColumn("interest_rate_id", col("interest_rate_uuid"))
    ir_df10 = ir_df9.withColumn("inactive_flag", lit("False"))
    ir_df11 = ir_df10.withColumn("source_type", lit("False"))
    ir_df12 = ir_df11.withColumn("cap_flag", lit("False"))
    ir_df13 = ir_df12.withColumn("source_uuid", col("loan_id"))

    # rename
    ir_df14 = ir_df13 \
        .withColumnRenamed("accounts_created_at", "created_time") \
        .withColumnRenamed("accounts_updated_at", "updated_time") \
        .withColumnRenamed("accounts_build_end_time", "end_date")

    # Selecting only the targrted columns
    selected_columns = ["loan_id", "loan_uuid", "interest_rate", "effective_date", "interest_rate_uuid", "interest_rate_id",
                          "inactive_flag", "created_time", "updated_time", "source_type", "source_uuid", "end_date", "cap_flag"]
    interest_rate_df = ir_df14.select(selected_columns)

    print("interest_rate succeeded")
    
    
    target = interest_rate_df.withColumn('extract_timestamp', lit(datetime.now()))

    #target.write.mode('overwrite').option("header", "true").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/interest_rate_transformation.parquet")

    target.coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/interest_rate_transformation.parquet")
    
def loan_collection_case_transformation():
    
    df = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    print('conversion started')
    old_names=convert(old_name)
    target_names=convert(target_name)
    print(old_names)
    print(target_names)
    mapping = dict(zip(old_names,target_names))
    df1=df.select([col(c).alias(mapping.get(c, c)) for c in df.columns])
    
    
    df1 = df.filter(df.dark == False)
    
     # Defining a window spec.
    window_spec = Window.partitionBy("loan_id").orderBy(df1.published_at.desc())

    # Adding a row number column to the df
    df_with_row_number = df1.withColumn("rn", row_number().over(window_spec))

    # Most recent record per loan_id
    df2 = df_with_row_number.filter("rn = 1")
    
    # Filter to remove any row where collection_cases= '[]'

    lcc_df3 = df2.filter(df2.collection_cases != '[]')
    

    # parse value from JSON: loan_id, loan_uuid
    json_schema = StructType([StructField("loan_id", StringType(), True)])
    df_parsed = lcc_df3.withColumn("loan_data_parsed", from_json(col("loan_data"), json_schema))
    lcc_df4 = df_parsed.withColumn("loan_id", df_parsed["loan_data_parsed.loan_id"])

    json_schema = StructType([StructField("loan_uuid", StringType(), True)])
    df_parsed = lcc_df4.withColumn("loan_data_parsed", from_json(col("loan_data"), json_schema))
    lcc_df5 = df_parsed.withColumn("loan_uuid", df_parsed["loan_data_parsed.loan_uuid"])

    # Explode collection_cases column to one row per owner per loan

    json_schema = ArrayType(StringType())
    lcc_df5 = lcc_df5.withColumn("collection_cases_array", from_json("collection_cases", json_schema))
    lcc_df5 = lcc_df5.select("*", explode("collection_cases_array").alias("loan_collection_case"))
    

    # Parse from collection_cases

    schema = ArrayType(StructType([
        StructField("case_type", StringType(), True),
        StructField("start_date", TimestampType(), True),
        StructField("end_date", TimestampType(), True),
        StructField("agency_name", StringType(), True),
        StructField("agency_email", StringType(), True),
        StructField("agency_phone", StringType(), True),
        StructField("poa_effective_date", TimestampType(), True),
    ]))


    df_parsed = lcc_df5.withColumn("cs_parsed", from_json(col("collection_cases"), schema))

    # Select the extracted fields as new columns
    lcc_df6 = df_parsed.select(
        "*",
        col("cs_parsed.case_type").alias("case_type"),
        col("cs_parsed.start_date").alias("start_date"),
        col("cs_parsed.end_date").alias("end_date"),
        col("cs_parsed.agency_name").alias("collection_agency_name"),
        col("cs_parsed.agency_email").alias("collection_agency_email"),
        col("cs_parsed.agency_phone").alias("collection_agency_phone"),
        col("cs_parsed.poa_effective_date").alias("poa_effective_date"),
    )

    def extract_array_column(df, column_name):
        return df.withColumn(column_name, col(column_name).getItem(0))

    columns_to_extract = ["case_type", "start_date", "end_date", "collection_agency_name",
                          "collection_agency_email", "collection_agency_phone", "poa_effective_date"
                          ]
    lcc_df7 = lcc_df6
    for column in columns_to_extract:
        lcc_df7 = extract_array_column(lcc_df7, column)

    # hash(loan_id + start_date)

    lcc_df8 = lcc_df7.withColumn("loan_collection_case_uuid", sha2(concat_ws("", lcc_df7["loan_id"], lcc_df7["start_date"]), 256))

    # Selecting only the targrted columns
    selected_columns = ["loan_id", "loan_uuid", "case_type", "start_date", "end_date", "collection_agency_name",
                          "collection_agency_email", "collection_agency_phone", "poa_effective_date", "loan_collection_case_uuid"]
    loan_collection_case_df = lcc_df8.select(selected_columns)

    print("loan_collection_case_transformation succeeded")
    
    
    target = loan_collection_case_df.withColumn('extract_timestamp', lit(datetime.now()))

    #target.write.mode('overwrite').option("header", "true").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/loan_collection_case_transformation.parquet")
    target.coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/loan_collection_case_transformation.parquet")

    
def loan_financial_owner_transformation():
    
    df = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    print('conversion started')
    old_names=convert(old_name)
    target_names=convert(target_name)
    print(old_names)
    print(target_names)
    mapping = dict(zip(old_names,target_names))
    df1=df.select([col(c).alias(mapping.get(c, c)) for c in df.columns])
    
    
    df1 = df.filter(df.dark == False)
    
    # Defining a window spec.
    window_spec = Window.partitionBy("loan_id").orderBy(df1.published_at.desc())

    # Adding a row number column to the df
    df_with_row_number = df1.withColumn("rn", row_number().over(window_spec))

    # Most recent record per loan_id
    df2 = df_with_row_number.filter("rn = 1")
    
    # Filter to remove any row where financial_owners = '[]'
    lfo_df3 = df2.filter(df2.financial_owners != '[]')
    

    # parse value from JSON: loan_id, loan_uuid

    json_schema = StructType([StructField("loan_id", StringType(), True)])
    df_parsed = df1.withColumn("loan_data_parsed", from_json(col("loan_data"), json_schema))
    lfo_df3 = df_parsed.withColumn("loan_id", df_parsed["loan_data_parsed.loan_id"])

    json_schema = StructType([StructField("loan_uuid", StringType(), True)])
    df_parsed = lfo_df3.withColumn("loan_data_parsed", from_json(col("loan_data"), json_schema))
    lfo_df4 = df_parsed.withColumn("loan_uuid", df_parsed["loan_data_parsed.loan_uuid"])

    # Explode financial_owners column to one row per owner per loan

    json_schema = ArrayType(StringType())
    lfo_df5 = lfo_df4.withColumn("financial_owners_array", from_json("financial_owners", json_schema))
    lfo_df5 = lfo_df5.select("*", explode("financial_owners_array").alias("financial_owners_explode"))

    # Parse from financial_owners

    schema = ArrayType(StructType([
        StructField("legal_entity", StringType(), True),
        StructField("financial_owner", StringType(), True),
        StructField("financial_owner_type", StringType(), True),
        StructField("uuid", StringType(), True),
        StructField("financial_owner_eff_date", TimestampType(), True),
        StructField("financial_owner_termination_date", TimestampType(), True),
        StructField("financial_owner_termination_reason", StringType(), True),
        StructField("marketplace_status", StringType(), True),
        StructField("marketplace_premium", DecimalType(), True),
        StructField("marketplace_final_principal_ar", DecimalType(), True),
        StructField("marketplace_final_interest_ar", DecimalType(), True),
    ]))

    df_parsed = lfo_df5.withColumn("fo_parsed", from_json(col("financial_owners"), schema))

    # Select the extracted fields as new columns
    lfo_df6 = df_parsed.select(
        "*",
        col("fo_parsed.legal_entity").alias("legal_entity"),
        col("fo_parsed.financial_owner").alias("financial_owner_name"),
        col("fo_parsed.financial_owner_type").alias("financial_owner_type"),
        col("fo_parsed.uuid").alias("financial_owner_uuid"),
        col("fo_parsed.financial_owner_eff_date").alias("effective_date"),
        col("fo_parsed.financial_owner_termination_date").alias("termination_date"),
        col("fo_parsed.financial_owner_termination_reason").alias("termination_reason"),
        col("fo_parsed.marketplace_status").alias("marketplace_status"),
        col("fo_parsed.marketplace_premium").alias("marketplace_premium"),
        col("fo_parsed.marketplace_final_principal_ar").alias("marketplace_final_principal_ar_amount"),
        col("fo_parsed.marketplace_final_interest_ar").alias("marketplace_final_interest_ar_amount"),
    )

    def extract_array_column(df, column_name):
        return df.withColumn(column_name, col(column_name).getItem(1))

    columns_to_extract = ["legal_entity", "financial_owner_name", "financial_owner_type", "financial_owner_uuid", "effective_date", "termination_date","termination_reason","marketplace_status", "marketplace_premium", "marketplace_final_principal_ar_amount", "marketplace_final_interest_ar_amount"
                          ]
    lfo_df7 = lfo_df6
    for column in columns_to_extract:
        lfo_df7 = extract_array_column(lfo_df7, column)

    # hash(loan_id + effective_date + legal_entity)

    lfo_df8 = lfo_df7.withColumn("loan_financial_owner_uuid", sha2(concat_ws("", lfo_df7["loan_id"], lfo_df7["effective_date"], lfo_df7["legal_entity"]), 256))

    # Selecting only the targrted columns
    selected_columns = ["loan_id", "loan_uuid", "legal_entity", "financial_owner_name", "financial_owner_type", "financial_owner_uuid", "effective_date", "termination_date","termination_reason","marketplace_status", "marketplace_premium", "marketplace_final_principal_ar_amount", "marketplace_final_interest_ar_amount", "loan_financial_owner_uuid"]
    loan_financial_owner_df = lfo_df8.select(selected_columns)

    print("loan_financial_owner_transformation succeeded")
    
    
    target = loan_financial_owner_df.withColumn('extract_timestamp', lit(datetime.now()))

    #target.write.mode('overwrite').option("header", "true").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/loan_financial_owner_transformation.parquet")

    target.coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/loan_financial_owner_transformation.parquet")
    
def operational_charge_off_transformation():
    
    df = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    print('conversion started')
    old_names=convert(old_name)
    target_names=convert(target_name)
    print(old_names)
    print(target_names)
    mapping = dict(zip(old_names,target_names))
    df1=df.select([col(c).alias(mapping.get(c, c)) for c in df.columns])
    
    
    df1 = df.filter(df.dark == False)
    
    # Defining a window spec.
    window_spec = Window.partitionBy("loan_id").orderBy(df1.published_at.desc())

    # Adding a row number column to the df
    df_with_row_number = df1.withColumn("rn", row_number().over(window_spec))

    # Most recent record per loan_id
    df2 = df_with_row_number.filter("rn = 1")
    
    # 3) parse value from JSON: loan_id, loan_uuid
    json_schema = StructType([StructField("loan_id", StringType(), True)])
    df_parsed = df2.withColumn("loan_data_parsed", from_json(col("loan_data"), json_schema))
    oco_df3 = df_parsed.withColumn("loan_id", df_parsed["loan_data_parsed.loan_id"])

    json_schema = StructType([StructField("loan_uuid", StringType(), True)])
    df_parsed = oco_df3.withColumn("loan_data_parsed", from_json(col("loan_data"), json_schema))
    oco_df4 = df_parsed.withColumn("loan_uuid", df_parsed["loan_data_parsed.loan_uuid"])

    # 4) Explode operational_charge_offs column to one row per charge off

    json_schema = ArrayType(StringType())
    oco_df5 = oco_df4.withColumn("operational_charge_offs_array", from_json("operational_charge_offs", json_schema))
    oco_df5 = oco_df5.select("*", explode("operational_charge_offs_array").alias("operational_charge_offs_explode"))
    

    # 5) Parse operational_charge_offs

    schema = ArrayType(StructType([
        StructField("uuid", StringType(), True),
        StructField("charge_off_date", TimestampType(), True),
        StructField("charge_off_reason", StringType(), True),
        StructField("forgiveness_date", TimestampType(), True),
        StructField("write_off_amount", DoubleType(), True),
        StructField("charge_off_amount", StructType([
            StructField("fees", DoubleType(), True),
            StructField("interest", DoubleType(), True),
            StructField("principal", DoubleType(), True),
        ]), True),
    ]))

    df_parsed = oco_df5.withColumn("oco_parsed", from_json(col("operational_charge_offs"), schema))

    # Select the extracted fields as new columns
    oco_df6 = df_parsed.select(
        "*",
        col("oco_parsed.uuid").alias("operational_charge_off_uuid"),
        col("oco_parsed.charge_off_date").alias("charge_off_date"),
        col("oco_parsed.charge_off_reason").alias("charge_off_reason"),
        col("oco_parsed.forgiveness_date").alias("forgiveness_date"),
        col("oco_parsed.write_off_amount").alias("write_off_amount"),
        col("oco_parsed.charge_off_amount.fees").alias("charge_off_fees_amount"),
        col("oco_parsed.charge_off_amount.interest").alias("charge_off_interest_amount"),
        col("oco_parsed.charge_off_amount.principal").alias("charge_off_principal_amount"),
    )

    def extract_array_column(df, column_name):
        return df.withColumn(column_name, col(column_name).getItem(0))

    columns_to_extract = ["operational_charge_off_uuid", "charge_off_date", "charge_off_reason", "forgiveness_date", "write_off_amount", "charge_off_fees_amount","charge_off_interest_amount","charge_off_principal_amount"
                          ]
    oco_df7 = oco_df6
    for column in columns_to_extract:
        oco_df7 = extract_array_column(oco_df7, column)

    # Selecting only the targrted columns
    selected_columns = ["loan_id", "loan_uuid", "operational_charge_off_uuid", "charge_off_date", "charge_off_reason", "servicing_account_id", "forgiveness_date", "write_off_amount", "charge_off_fees_amount","charge_off_interest_amount","charge_off_principal_amount"]
    operational_charge_off_df = oco_df7.select(selected_columns)
    print("operational_charge_off_transformation succeeded")
    
    
    target = operational_charge_off_df.withColumn('extract_timestamp', lit(datetime.now()))

    #target.write.mode('overwrite').option("header", "true").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/operational_charge_off_transformation.parquet")
    target.coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/operational_charge_off_transformation.parquet")
    
def original_installment_transformation():
    
    df = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    print('conversion started')
    old_names=convert(old_name)
    target_names=convert(target_name)
    print(old_names)
    print(target_names)
    mapping = dict(zip(old_names,target_names))
    df1=df.select([col(c).alias(mapping.get(c, c)) for c in df.columns])
    
    
    df1 = df.filter(df.dark == False)
    
    # Defining a window spec.
    window_spec = Window.partitionBy("loan_id").orderBy(df1.published_at.desc())

    # Adding a row number column to the df
    df_with_row_number = df1.withColumn("rn", row_number().over(window_spec))

    # Most recent record per loan_id
    df2 = df_with_row_number.filter("rn = 1")
    
    # 3) Filter to remove any row where original_installments= '[]'

    oi_df3 = df2.filter(df2.original_installments != '[]')
    

    # 4) parse value from JSON : loan_id, loan_uuid

    json_schema = StructType([StructField("loan_id", StringType(), True)])
    df_parsed = df1.withColumn("loan_data_parsed", from_json(col("loan_data"), json_schema))
    oi_df3 = df_parsed.withColumn("loan_id", df_parsed["loan_data_parsed.loan_id"])

    json_schema = StructType([StructField("loan_uuid", StringType(), True)])
    df_parsed = oi_df3.withColumn("loan_data_parsed", from_json(col("loan_data"), json_schema))
    oi_df4 = df_parsed.withColumn("loan_uuid", df_parsed["loan_data_parsed.loan_uuid"])

    # 5) Explode original_installments column to one row per owner per loan

    json_schema = ArrayType(StringType())
    oi_df5 = oi_df4.withColumn("original_installments_array", from_json("original_installments", json_schema))
    oi_df5 = oi_df5.select("*", explode("original_installments_array").alias("original_installments_explode"))
    

    schema = ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("installment_date", TimestampType(), True),
        StructField("original_interest", DecimalType(), True),
        StructField("original_principal", DecimalType(), True),
        StructField("installment_schedule_id", StringType(), True),
    ]))

    df_parsed = oi_df5.withColumn("oi_parsed", from_json(col("original_installments"), schema))

    # Select the extracted fields as new columns
    oi_df6 = df_parsed.select(
        "*",
        col("oi_parsed.id").alias("original_installment_id"),
        col("oi_parsed.installment_date").alias("installment_date"),
        col("oi_parsed.original_interest").alias("interest_amount"),
        col("oi_parsed.original_principal").alias("principal_amount"),
        col("oi_parsed.installment_schedule_id").alias("installment_schedule_id"),
    )

    def extract_array_column(df, column_name):
        return df.withColumn(column_name, col(column_name).getItem(0))

    columns_to_extract = ["original_installment_id", "installment_date", "interest_amount", "principal_amount", "installment_schedule_id"
                          ]
    oi_df7 = oi_df6
    for column in columns_to_extract:
        oi_df7 = extract_array_column(oi_df7, column)

    oi_df8 = oi_df7.withColumn("original_installment_uuid", sha2(col("id"), 256))

    # Selecting only the targrted columns
    selected_columns = ["loan_id", "loan_uuid", "original_installment_id", "installment_date", "interest_amount", "principal_amount", "installment_schedule_id", "original_installment_uuid"]
    oi_df9 = oi_df8.select(selected_columns)
    print("original_installment_transformation succeeded")
    
    original_installment_df = oi_df9.dropDuplicates()
    
    
    target = original_installment_df.withColumn('extract_timestamp', lit(datetime.now()))

    #target.write.mode('overwrite').option("header", "true").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/original_installment_transformation.parquet")
    target.coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/original_installment_transformation.parquet")

    
def treasury_charge_off_transformation():
    
    df = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    print('conversion started')
    old_names=convert(old_name)
    target_names=convert(target_name)
    print(old_names)
    print(target_names)
    mapping = dict(zip(old_names,target_names))
    df1=df.select([col(c).alias(mapping.get(c, c)) for c in df.columns])
    
    
    df1 = df.filter(df.dark == False)
    
    # Defining a window spec.
    window_spec = Window.partitionBy("loan_id").orderBy(df1.published_at.desc())

    # Adding a row number column to the df
    df_with_row_number = df1.withColumn("rn", row_number().over(window_spec))

    # Most recent record per loan_id
    df2 = df_with_row_number.filter("rn = 1")
    
    # 3) parse value from JSON: loan_id, loan_uuid
    json_schema = StructType([StructField("loan_id", StringType(), True)])
    df_parsed = df2.withColumn("loan_data_parsed", from_json(col("loan_data"), json_schema))
    tco_df3 = df_parsed.withColumn("loan_id", df_parsed["loan_data_parsed.loan_id"])

    json_schema = StructType([StructField("loan_uuid", StringType(), True)])
    df_parsed = tco_df3.withColumn("loan_data_parsed", from_json(col("loan_data"), json_schema))
    tco_df4 = df_parsed.withColumn("loan_uuid", df_parsed["loan_data_parsed.loan_uuid"])

    # 5) Explode treasury_charge_off_reason column to one row per charge off

    schema = ArrayType(StructType([
        StructField("uuid", StringType(), True),
        StructField("effective_date", TimestampType(), True),
        StructField("charge_off_reason", StringType(), True),
        StructField("transaction_timestamp", TimestampType(), True),
        StructField("id", StringType(), True),
        StructField("reverted_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("source_uuid", StringType(), True),
        StructField("source_type", StringType(), True),
        StructField("revertible", BooleanType(), True),
    ]))

    df_parsed = tco_df4.withColumn("tco_parsed", from_json(col("treasury_charge_offs"), schema))

    tco_df5 = df_parsed.select(
        "*",
        col("tco_parsed.uuid").alias("treasury_charge_off_uuid"),
        col("tco_parsed.effective_date").alias("effective_date"),
        col("tco_parsed.charge_off_reason").alias("charge_off_reason"),
        col("tco_parsed.transaction_timestamp").alias("created_time"),
        col("tco_parsed.id").alias("treasury_charge_off_id"),
        col("tco_parsed.reverted_at").alias("reverted_time"),
        col("tco_parsed.updated_at").alias("updated_time"),
        col("tco_parsed.source_uuid").alias("source_uuid"),
        col("tco_parsed.source_type").alias("source_type"),
        col("tco_parsed.revertible").alias("revertible_flag"),
    )

    def extract_array_column(df, column_name):
        return df.withColumn(column_name, col(column_name).getItem(0))

    columns_to_extract = ["treasury_charge_off_uuid", "effective_date", "charge_off_reason", "created_time", "treasury_charge_off_id", "reverted_time","updated_time","source_uuid", "source_type", "revertible_flag"
                          ]
    tco_df6 = tco_df5
    for column in columns_to_extract:
        tco_df6 = extract_array_column(tco_df6, column)

    # Selecting only the targrted columns
    selected_columns = ["loan_id", "loan_uuid","treasury_charge_off_uuid", "servicing_account_id", "effective_date", "charge_off_reason", "created_time", "treasury_charge_off_id", "reverted_time","updated_time","source_uuid", "source_type", "revertible_flag"]
    treasury_charge_off_df = tco_df6.select(selected_columns)
    print("treasury_charge_off_transformation succeeded")
    
    
    target = treasury_charge_off_df.withColumn('extract_timestamp', lit(datetime.now()))

    #target.write.mode('overwrite').option("header", "true").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/treasury_charge_off_transformation.parquet")
    target.coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/treasury_charge_off_transformation.parquet")

def loan_payment_transformation():
    def conversion(my_data):
            if my_data == "[]" or my_data is None:
                return None
            data = json.loads(my_data)
            if isinstance(data, dict):
                return [my_data]
            elif isinstance(data, list):
                return [json.dumps(each) for each in data]
            else:
                return None

    uil_df7 = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    
    conversion_udf = udf(conversion, ArrayType(StringType()))
    uil_df7 = uil_df7.filter(uil_df7.dark == False)
    uil_df7 = uil_df7.filter(uil_df7.payment_plans != '[]')

    list_of_attributes_loan_data = ['loan_id', 'loan_uuid', 'payment_method','origination_state']
    for col in list_of_attributes_loan_data:
        uil_df7 = uil_df7.withColumn(col, get_json_object(uil_df7.loan_data, f"$.{col}"))


    window = Window.partitionBy('loan_id').orderBy(uil_df7.portfolios_created_at.desc())
    uil_df7 = uil_df7.withColumn("rownum", row_number().over(window))
    uil_df7 = uil_df7.filter(uil_df7.rownum == 1)

    list_of_attributes_customer_information = ['customer_id']
    for col in list_of_attributes_customer_information:
        uil_df7 = uil_df7.withColumn(col, get_json_object(uil_df7.customer_information, f"$.{col}"))


    uil_df7 = uil_df7.withColumn('temp_explode_payment_plans', conversion_udf(uil_df7.payment_plans))
    uil_df7 = uil_df7.select("*", explode(uil_df7.temp_explode_payment_plans).alias("explode_payment_plans"))

    list_of_attributes_payment_plans = ['status','lock_in_date','total_amount','payment_amount','adjusted_end_date',
                                        'first_payment_date','early_terminate_date','adjusted_lock_in_date','tdr',
                                        'id','start_date','end_date','created_timestamp','plan_type','uuid','frequency',
                                        'contract_id','contract_signed_datetime','created_by_id','payment_dates','rule_version',
                                        'freezes_lateness', 'lateness_reset_date', 'updated_at']

    for col in list_of_attributes_payment_plans:
        uil_df7 = uil_df7.withColumn(col, get_json_object(uil_df7.explode_payment_plans, f"$.{col}"))



    uil_df7 = uil_df7\
        .withColumn("structure", uil_df7.plan_type)\
        .withColumn("created_time", from_utc_timestamp(uil_df7.created_timestamp, 'CST'))

    uil_df7 = uil_df7 \
        .withColumn("grace_period_days", when(uil_df7.origination_state.isin('CA', 'MO'), lit(15)).otherwise(lit(10)).cast("int")) \
        .withColumn("loan_uuid", uil_df7.loan_uuid)\
        .withColumn("payment_method", uil_df7.payment_method)\
        .withColumn("customer_id", uil_df7.customer_id.cast("int"))\
        .withColumn("loan_id", uil_df7.loan_id.cast("int"))\
        .withColumn("lock_in_date", uil_df7.lock_in_date.cast("date"))\
        .withColumn("loan_payment_plan_id", uil_df7.id.cast("int"))\
        .withColumn("contract_id", uil_df7.contract_id.cast("int"))\
        .withColumn("created_by_portal_id", uil_df7.created_by_id.cast("int"))\
        .withColumn("contract_signed_date", uil_df7.contract_signed_datetime.cast("date"))\
        .withColumn("rule_version", uil_df7.rule_version)\
        .withColumn("total_amount", uil_df7.total_amount.cast("decimal(12,2)"))\
        .withColumn("payment_amount", uil_df7.payment_amount.cast("decimal(12,2)"))\
        .withColumn("completed_date",
                    when(~(uil_df7.structure.isin('loan_mod_trial', 'term_extension_trial')) & (uil_df7.status == "completed"),
                         coalesce(uil_df7.adjusted_end_date.cast("date"), uil_df7.end_date.cast("date")))\
                    .otherwise(None).cast("date"))\
        .withColumn("adjusted_lock_in_date",
                    when((uil_df7.structure.isin('loan_mod_trial', 'term_extension_trial')) & (uil_df7.status == "completed"),
                         coalesce(uil_df7.adjusted_end_date, uil_df7.end_date))\
                    .otherwise(uil_df7.adjusted_lock_in_date).cast("date"))\
        .withColumn("adjusted_end_date",
                    when((uil_df7.structure.isin('loan_mod_trial', 'term_extension_trial')) & (uil_df7.status == "terminated"), uil_df7.early_terminate_date)\
                    .when(~(uil_df7.structure.isin('loan_mod_trial', 'term_extension_trial')),
                          coalesce(uil_df7.adjusted_end_date, uil_df7.early_terminate_date, uil_df7.end_date))\
                    .otherwise(None).cast("date"))\
        .withColumn("first_payment_date", uil_df7.first_payment_date.cast("date"))\
        .withColumn("early_terminate_date", uil_df7.early_terminate_date.cast("date"))\
        .withColumn("start_date",
                    when(uil_df7.structure == 'deferment', uil_df7.created_time.cast("date"))\
                    .when(uil_df7.structure == 'long_term', uil_df7.created_time.cast("date"))\
                    .otherwise(uil_df7.start_date).cast("date"))\
        .withColumn("end_date",
                    when(~(uil_df7.structure.isin('loan_mod_trial', 'term_extension_trial')), uil_df7.end_date)\
                    .otherwise(None).cast("date"))\
        .withColumn("status",
                    when((uil_df7.structure.isin('loan_mod_trial', 'term_extension_trial')) & (uil_df7.status == "completed"), 'locked_in')\
                    .otherwise(uil_df7.status))\
        .withColumn("structure",
                    when(uil_df7.structure == 'loan_mod_trial', 'apr_reduction')\
                    .when(uil_df7.structure == 'term_extension_trial','term_extension')\
                    .otherwise(uil_df7.structure))\
        .withColumn("loan_payment_plan_uuid", uil_df7.uuid)\
        .withColumn("payment_dates_text", uil_df7.payment_dates)\
        .withColumn("frequency", uil_df7.frequency)\
        .withColumn("troubled_debt_restructuring_flag", uil_df7.tdr.cast("boolean"))\
        .withColumn("freezes_lateness_flag", uil_df7.freezes_lateness.cast("boolean"))\
        .withColumn("lateness_reset_date", uil_df7.lateness_reset_date.cast("date"))\
        .withColumn("updated_time", from_utc_timestamp(uil_df7.updated_at, 'CST'))


    lpp_select_cols = ['loan_id','loan_uuid','payment_method','customer_id','grace_period_days','status',
                       'lock_in_date','total_amount','payment_amount','adjusted_end_date','first_payment_date',
                       'early_terminate_date','adjusted_lock_in_date','troubled_debt_restructuring_flag',
                       'loan_payment_plan_id','start_date','end_date','created_time','structure','loan_payment_plan_uuid',
                       'frequency','contract_id','contract_signed_date','created_by_portal_id','payment_dates_text',
                       'rule_version','completed_date', 'lateness_reset_date', 'freezes_lateness_flag',
                       'etl_ingestion_time', 'source_file_creation_time', 'updated_time']

    loan_payment_plans_df = uil_df7.select(lpp_select_cols)
    
    print("loan_payment_transformation successful")
    # Convert DataFrame to list of dictionaries
    serialized_data = [row.asDict() for row in loan_payment_plans_df.collect()]
    
    return serialized_data
    print("loan_payment_transformation succeeded")
    
    target = loan_payment_plans_df.withColumn('extract_timestamp', lit(datetime.now()))

    #target.write.mode('overwrite').option("header", "true").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/loan_payment_plans_transformation.parquet")
    target.coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket_name}/transform_data/refi/{current_date1}/loan_payment_plans_transformation.parquet")

start = DummyOperator(task_id='Start', dag=dag)

watch_for_file_drop = ShortCircuitOperator(
    task_id='watch_for_file_drop',
    python_callable=check_file_in_s3,
    provide_context=True,
    dag=dag,
)

validation_schema = PythonOperator(
    task_id='validation_schema',
    python_callable=compare_schema_func,
    dag=dag,
)

loan_debt_sale = PythonOperator(
    task_id = 'loan_debt_sale',
    python_callable=loan_debt_sale_transformation,
    dag=dag,
)

loan_servicing_settlement = PythonOperator(
    task_id = 'loan_servicing_settlement',
    python_callable=loan_servicing_settlement_transformation,
    dag=dag,
)

active_installment = PythonOperator(
    task_id = 'active_installment',
    python_callable=active_installment_transformation,
    dag=dag,
)

bankruptcy = PythonOperator(
    task_id = 'bankruptcy',
    python_callable=bankruptcy_transformation,
    dag=dag,
)

interest_rate = PythonOperator(
    task_id = 'interest_rate',
    python_callable=interest_rate_transformation,
    dag=dag,
)

loan_collection_case = PythonOperator(
    task_id = 'loan_collection_case',
    python_callable=loan_collection_case_transformation,
    dag=dag,
)

loan_financial_owner = PythonOperator(
    task_id = 'loan_financial_owner',
    python_callable=loan_financial_owner_transformation,
    dag=dag,
)

operational_charge_off = PythonOperator(
    task_id = 'operational_charge_off',
    python_callable=operational_charge_off_transformation,
    dag=dag,
)

original_installment = PythonOperator(
    task_id = 'original_installment',
    python_callable=original_installment_transformation,
    dag=dag,
)

treasury_charge_off = PythonOperator(
    task_id = 'treasury_charge_off',
    python_callable=treasury_charge_off_transformation,
    dag=dag,
)

loan_payment_plan = PythonOperator(
    task_id = 'loan_payment_plan',
    python_callable=loan_payment_transformation,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

start >> watch_for_file_drop >> validation_schema >> [loan_debt_sale, loan_servicing_settlement, active_installment, bankruptcy, interest_rate, loan_collection_case, loan_financial_owner, operational_charge_off, original_installment, treasury_charge_off, loan_payment_plan] >> end
#start >> check_s3_file_task >> conversion_task
