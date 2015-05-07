# pyspark-csv
An external PySpark module that works like R's read.csv or Panda's read_csv, with 
automatic type inference and null value handling. Parses csv data into SparkSQL DataFrames. No installation required, simply include **pyspark_csv.py** via SparkContext.

## Synopsis
Supports type inference by evaluating data within each column. In the case of column having multiple data types, **pyspark-csv** will assign the lowest common denominator type for that column. For example,
```
  Name,   Model,  Size, Width,  Dt
  Jag,    63,     4,    4,      '2014-12-23'
  Pog,    7.0,    5,    5,      '2014-12-23'
  Peek,   68xp,   5,    5.5,    ''
```
generates DataFrame with the following schema: 
```
  csv_file 
  |--Name: string  
  |--Model: string
  |--Size: int
  |--Width: double
  |--Dt: timestamp
```

## Usage
Required Python packages: **pyspark**, **csv**, **dateutil**

Assume we have the following context
```
  sc = SparkContext
  sqlCtx = SQLContext or HiveContext
```

First, distribute **pyspark-csv.py** to executors using SparkContext
```
import pyspark_csv as pycsv
sc.addPyFile('pyspark_csv.py')
```
Read csv data via SparkContext and convert it to DataFrame
```
plaintext_rdd = sc.textFile('hdfs://x.x.x.x/blah.csv')
dataframe = pycsv.csvToDataFrame(sqlCtx, plaintext_rdd)
```
By default, pyspark-csv parses the first line as column names. To supply your own column names
```
plaintext_rdd = sc.textFile('hdfs://x.x.x.x/blah.csv')
dataframe = pycsv.csvToDataFrame(sqlCtx, plaintext_rdd, columns=['Name','Model','Size','Width','Dt'])
```
To convert DataFrames to RDDs, call the .rdd method.

To change separator
```
plaintext_rdd = sc.textFile('hdfs://x.x.x.x/blah.csv')
datarame = pycsv.csvToDataFrame(sqlCtx, plaintext_rdd, sep=",")
```
Skipping date and time parsing can lead to significant performance gain on large datasets
```
plaintext_rdd = sc.textFile('hdfs://x.x.x.x/blah.csv')
dataframe = pycsv.csvToDataFrame(sqlCtx, plaintext_rdd, parseDate=False)
```
Currently, the following data types are supported:
- int
- double
- string
- date
- time
- datetime

It also recognises **None**, **?**, **NULL**, and **''** as null values

## Need help?
Contributors welcomed! Contact seah_boon_siew@ida.gov.sg
