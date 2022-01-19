# Retail_Streaming_Analysis

Language Used : Python
Tools Used : HDFS , Kafka Streaming , Spark Streaming

## Problem Statement

 Computing various Key Performance Indicators (KPIs) for an e-commerce company, RetailCorp Inc. You have been provided real-time sales data of the company across the globe. The data contains information related to the invoices of orders placed by customers all around the world.

## Tasks Performed

 1. Reading the sales data from the Kafka server
 
 2. Preprocessing the data to calculate additional derived columns such as total_cost etc
 3. Calculating the time-based KPIs and time and country-based KPIs
 4. Storing the KPIs (both time-based and time- and country-based) for a 10-minute interval into separate JSON files for further analysis

## Example of Data Used:

```
  {
  "invoice_no": 154132541653705,
  "country": "United Kingdom",
  "timestamp": "2020-09-18 10:55:23",
  "type": "ORDER",
  "items": [
    {
      "SKU": "21485",
      "title": "RETROSPOT HEART HOT WATER BOTTLE",
      "unit_price": 4.95,
      "quantity": 6
    },
    {
      "SKU": "23499",
      "title": "SET 12 VINTAGE DOILY CHALK",
      "unit_price": 0.42,
      "quantity": 2
    }
  ]  
}
```
