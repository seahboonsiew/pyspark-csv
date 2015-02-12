# pyspark-csv
A PySpark module for seamless reading of csv file into SchemaRDD. It works like R's read.csv or Panda read_csv with 
automatic type inference.

Supports type inference by evaluating data within each column. In the case where multiple data types are encountered 
within a column, pyspark-csv will assign the lowest common denominator type. For example, this csv dataset:

name, model
Jag, 63
Pog, 7.0
Peek, 68xp

will generate the following schema:
{
  name: String,
  model: String
}

Currently, the following data types are support:
1) int
2) double
3) string
4) date
5) time
6) datetime
