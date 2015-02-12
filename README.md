# pyspark-csv
An external PySpark module for parsing csv file into SchemaRDD. It works like R's read.csv or Panda read_csv with 
automatic type inference.

## Synopsis
Supports type inference by evaluating data within each column. In the case where multiple data types are encountered, **pyspark-csv** will assign the lowest common denominator type for that column. For example,
```
  Name,   Model,  Size, Width,  Dt
  Jag,    63,     4,    4,      '2014-12-23'
  Pog,    7.0,    5,    5,      '2014-12-23'
  Peek,   68xp,   5,    5.5,    ''
```
will generate SchemaRDD with the following schema: 
```
  csv_file 
  |--Name: string  
  |--Model: string
  |--Size: int
  |--Width: double
  |--Dt: datetime.date
```

## Usage
Required Python packages: **pyspark**, **csv**, **dateutil**

Assume we have the following context
```
  sc = SparkContext
  sqlCtx = SQLContext or HiveContext
```

First, include **pyspark-csv.py** using SparkContext
```
import pyspark_csv as pycsv
sc.addPyFile('pyspark_csv.py')
```
Read csv data via SparkContext and convert it to SchemaRDD via pyspark-csv
```
raw_text = sc.textFile('hdfs://x.x.x.x/blah.csv')
raw_with_schema = pycsv.csvToRDD(sqlCtx, hansard)
```

Currently, the following data types are support:
- int
- double
- string
- date
- time
- datetime

It also recognises **None**, **?** and **''** as null values

## Need help?
Contact seah_boon_siew@ida.gov.sg
