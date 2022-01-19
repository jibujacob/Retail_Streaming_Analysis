#Pre-Requisites Done:
# Validate the kafka topic consumer for due diligence
# For ingesting data from Kafka ,that are not present in the Spark Streaming core API , we will have to add
#   spark-streaming-kafka-0-10_2.12 artifacts .
#   Run the command : wget https://ds-spark-sql-kafka-jar.s3.amazonaws.com/spark-sql-kafka-0-10_2.11-2.3.0.jar in the instance we are running this code
#   Run the command : export SPARK_KAFKA_VERSION=0.10 (inorder to set  the version else code will break with error)


## Step 1 : Setting up the dependancies by importing the necessary packages, setting up necessary functions to create the UDF later and ensuring the kafka-spark 
##          jar file artifact is downloaded in the instance where we run this file
# Importing required functions/packages
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# UDF1 to determine Is_Order in case of Order.
def isOrder(orderType):
    if orderType == "ORDER":
        return(1)
    else:
        return(0)
		
# UDF2 to determine Is_Return in case of Return.
def isReturn(orderType):
    if orderType == "RETURN":
        return(1) 
    else:
        return(0)
		
# UDF3 to determine Total cost per order

def itemOrderTotal(unit_price_arr,quantity_arr,order_type):
    sum = 0
    for i in range(0,len(unit_price_arr)):
        sum += unit_price_arr[i] * quantity_arr[i]
    if (order_type == "ORDER"):
        return sum
    else:
        return sum * -1

# UDF4 to determine total Quantity per order
def itemQuantityTotal(quantity_arr):
    total = 0 
    for i in range(0,len(quantity_arr)):
        total += quantity_arr[i]
    return total


## Step 2 : Setting up the spark session 
# Establishing Spark Session
spark = SparkSession  \
        .builder  \
        .appName("RetailStreamingAnalysis")  \
        .getOrCreate()

spark.sparkContext.setLogLevel('ERROR') 

## Step 3 : Setting up spark to read from Kafka server 
# Kafka Details Provided
#       Bootstrap Server - 18.211.252.152
#       Port - 9092
#       Topic - real-time-project
# Subscribing to above kafka topic to consume the retail streaming data
#Fetching the latest entries to avoid the processing the old data and get less cluttered output to ease differentiation
lines = spark  \
    .readStream  \
    .format("kafka")  \
    .option("kafka.bootstrap.servers","18.211.252.152:9092")  \
    .option("subscribe","real-time-project")  \
    .option("failOnDataLoss","false") \
    .option("startingOffsets","latest")  \
    .load()

## Step 4 : Converting incoming Raw Data to string format for processing
# Incoming data in column value from kakfa was in raw format need to transform it to string 
# Defining Schema based on the input provided
schema =  StructType([
    StructField("country", StringType()),
    StructField("invoice_no", LongType()) ,
    StructField("items", ArrayType(
        StructType([
            StructField("SKU", StringType()),
            StructField("title", StringType()),
            StructField("unit_price", FloatType()),
            StructField("quantity", IntegerType())
                  ])
            )),
    StructField("timestamp", TimestampType()),
    StructField("type", StringType()),  
])

# Casting column value raw data as string and aliasing                              
rawToStringCastDF = lines.selectExpr('CAST(value AS STRING)') \
    .select(from_json('value', schema).alias("value"))
    

## Step 5 : Flattenging the items columns values 
# Using the UDF fetching hte total order cost and total quantity per order
flattenedDF = rawToStringCastDF.select("value.country","value.invoice_no","value.items.unit_price","value.items.quantity","value.timestamp","value.type")

item_Order_TotalCost = udf(itemOrderTotal,FloatType())
order_TotalQuantity = udf(itemQuantityTotal,IntegerType())

flattenedDF=flattenedDF.withColumn("cost",item_Order_TotalCost(flattenedDF.unit_price,flattenedDF.quantity,flattenedDF.type))
flattenedDF=flattenedDF.withColumn("total_items",order_TotalQuantity(flattenedDF.quantity))

#Commenting the below logic as mulitple aggregation not allowed 
# # Using the explode function to flatten out the nested json object(items)
# explodeddDF = extactedDF.select(col("type"),
#                         col("country"),
#                         col("invoice_no"),
#                         col("timestamp"),
#                         explode(col("items")).alias("item"))

# # Columns from Items array displayed
# flattenedDF = explodeddDF.select("type","country" , "invoice_no", "timestamp", "item.SKU","item.title","item.unit_price","item.quantity")

# # Renaming the exploded json map objects
# # Array Columns renamed 
# flattenedDF = flattenedDF.withColumnRenamed("col.SKU","SKU")
# flattenedDF = flattenedDF.withColumnRenamed("col.title","title")
# flattenedDF = flattenedDF.withColumnRenamed("col.unit_price","unit_price")
# flattenedDF = flattenedDF.withColumnRenamed("col.quantity","quantity")

## Step 6 : Pre-processing Readiness : Creating the UDF needed for the KPI generation  


# UDF created to check if typ of transaction is Order or not
is_Order = udf(isOrder,IntegerType())

# UDF created to check if typ of transaction is Order or not
is_Return = udf(isReturn,IntegerType())

# Adding the total_cost,is_order,is_return columns in the flatttenedDF

flattenedDF=flattenedDF.withColumn("is_order",is_Order(flattenedDF.type))
flattenedDF=flattenedDF.withColumn("is_return",is_Return(flattenedDF.type))
flattenedDF=flattenedDF.withColumn("ts",flattenedDF.timestamp)


##Step 7 : Generating source table for KPI and the finalized summarazed input for console
# flattenedDF to be our base for our KPI's
finalInputValuesDF = flattenedDF.select("invoice_no","country","timestamp","ts","cost","total_items","is_order","is_return")


# # Final Summarized Input Value for console , adding watermark to ensure load is less and remove unnecessary items
finalInputValueswindowedDF = finalInputValuesDF.withWatermark("ts","10 minutes") \
		.groupby(window("ts","1 minute"),"invoice_no","timestamp","country","is_order","is_return") \
        .agg(sum("cost").alias("total_cost"),sum("total_items").alias("total_items"))

# Formatting the console output display , adding the window for ease of comparing
finalInputValueswindowedDF = finalInputValueswindowedDF.select("window","invoice_no","country","timestamp","total_cost","total_items","is_order","is_return")


## Step 8 : Generating the Time based KPI 
# Selecting columns that we need for the time based KPI
timeBasedInput = finalInputValuesDF.select("invoice_no","timestamp","cost","total_items","is_order","is_return")

# Setting the tumbling window of 1 min for the incoming transactions , and pre-processing the necessary columns 
timeBasedKPI = timeBasedInput.withWatermark("timestamp","10 minutes") \
            .groupby(window("timestamp","1 minute")) \
            .agg(round(sum("cost"),2).alias("total_sale_volume"), \
                approx_count_distinct("invoice_no").alias("OPM"), \
                sum("total_items").alias("total_items"), \
                sum("is_order").alias("total_order"), \
                sum("is_return").alias("total_return"), \
                )

# KPI for rate of return
timeBasedKPI = timeBasedKPI.withColumn("rate_of_return",round(timeBasedKPI.total_return/(timeBasedKPI.total_order+timeBasedKPI.total_return),2))	

# KPI for average transaction size	
timeBasedKPI = timeBasedKPI.withColumn("average_transaction_size",round(timeBasedKPI.total_sale_volume/(timeBasedKPI.total_order+timeBasedKPI.total_return),2))

# Formatting the log output display
timeBasedKPI = timeBasedKPI.select("window","OPM","total_sale_volume","average_transaction_size","rate_of_return")	


## Step 9 : Generating the Time and Country based KPI 
# Selecting columns that we need for the time-country based KPI
timeCountryBasedInput = finalInputValuesDF.select("invoice_no","country","timestamp","cost","total_items","is_order","is_return")

# Setting the tumbling window of 1 min for the incoming transactions , and pre-processing the necessary columns 
timeAndCountryBasedKPI = timeCountryBasedInput.withWatermark("timestamp","10 minutes") \
            .groupby(window("timestamp","1 minute"),"country") \
            .agg(round(sum("cost"),2).alias("total_sale_volume"), \
                approx_count_distinct("invoice_no").alias("OPM"), \
                sum("total_items").alias("total_items"), \
                sum("is_order").alias("total_order"), \
                sum("is_return").alias("total_return"), \
                )

# KPI for rate of return
timeAndCountryBasedKPI = timeAndCountryBasedKPI.withColumn("rate_of_return",round(timeAndCountryBasedKPI.total_return/(timeAndCountryBasedKPI.total_order+timeAndCountryBasedKPI.total_return),2))	

# Formatting the log output display
timeAndCountryBasedKPI = timeAndCountryBasedKPI.select("window","country","OPM","total_sale_volume","rate_of_return")	


## Step 10 : Generating the outputs for the input and the KPI's
# Printing the FinalSummarizedInput Value into the console
finalSummarizedInputQuery = finalInputValueswindowedDF  \
	.writeStream  \
	.outputMode("append")  \
    .option("truncate","false") \
	.format("console")  \
	.start()

# Print time based KPI to HDFS path as a JSON
timeKPIJSONQuery = timeBasedKPI.writeStream \
    .outputMode("Append") \
    .format("json") \
    .option("format","append") \
    .option("truncate", "false") \
    .option("path","time_KPI") \
    .option("checkpointLocation", "time_KPI_json") \
    .trigger(processingTime="1 minute") \
    .start()

# Print time and country based KPI to HDFS path as a JSON
timeCountryKPIJSONQuery = timeAndCountryBasedKPI.writeStream \
    .outputMode("Append") \
    .format("json") \
    .option("format","append") \
    .option("truncate", "false") \
    .option("path","time_country_KPI") \
    .option("checkpointLocation", "time_country_KPI_json") \
    .trigger(processingTime="1 minute") \
    .start()

# Termination Call for the Queries
finalSummarizedInputQuery.awaitTermination()
timeKPIJSONQuery.awaitTermination()
timeCountryKPIJSONQuery.awaitTermination()
