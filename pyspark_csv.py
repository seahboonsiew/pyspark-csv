"""
The MIT License (MIT)

Copyright (c) 2015 seahboonsiew

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import csv
import dateutil.parser
from pyspark import sql
from pyspark.sql import *
from pyspark.sql.types import *

"""
Convert CSV plain text RDD into SparkSQL DataFrame (former SchemaRDD) using PySpark
If columns not given, assume first row is the header
If separator not given, assume comma separated
"""
def csvToDataFrame(sqlCtx,rdd,columns=None,sep=",",parseDate=True):
    def toRow(line):
        return toRowSep(line,sep)
    rdd_array = rdd.map(toRow)
    rdd_sql = rdd_array
    if columns is None:
        columns = rdd_array.first()
        rdd_sql = rdd_array.zipWithIndex().filter(lambda (r,i): i > 0).keys()
    column_types = evaluateType(rdd_sql,parseDate)
    def toSqlRow(row):
        return toSqlRowWithType(row,column_types)
    schema = makeSchema(zip(columns,column_types)) 
    return  sqlCtx.createDataFrame(rdd_sql.map(toSqlRow), schema=schema)

def makeSchema(columns):
    struct_field_map = { 'string':StringType(), 'date': TimestampType(), 'double': DoubleType(), 'int': IntegerType(), 'none':NullType()}
    fields = [StructField(k, struct_field_map[v], True) for k,v in columns]
    return StructType(fields)

# Parse a row using csv.reader	
def toRowSep(line,d):
    for r in csv.reader([line.encode('utf-8')], delimiter=d):
        return r 
        

# Actual conversion to sql.Row
def toSqlRowWithType(row,col_types):
    d = row 
    for col,data in enumerate(row):
        typed = col_types[col]
        if isNone(data):
            d[col] = None
        elif typed == 'string':
            d[col] = data
        elif typed == 'int':
            d[col] = int(round(float(data)))
        elif typed =='double': 
            d[col] = float(data)
        elif typed == 'date':
            d[col] = toDate(data)
    return d
	
# Type converter
def isNone(d):
    return (d is None or d == 'None' or d == '?' or d == '' or d == 'NULL' or d == 'null')

def toDate(d):
    return dateutil.parser.parse(d)

# Infer types for each row
def getRowType(row):
    d = row 
    for col,data in enumerate(row):        
        try:
            if isNone(data):
                d[col] = 'none'
            else:
                num = float(data) 
                if num.is_integer():
                    d[col] = 'int'            
                else:
                    d[col] = 'double'
        except:
            try:
                dt = toDate(data)
                d[col] = 'date'
            except:
                dt = data
                d[col] = 'string'
    return d
    
# Infer types for each row
def getRowTypeNoDate(row):
    d = row 
    for col,data in enumerate(row):        
        try:
            if isNone(data):
                d[col] = 'none'
            else:
                num = float(data) 
                if num.is_integer():
                    d[col] = 'int'            
                else:
                    d[col] = 'double'
        except:
            d[col] = 'string'
    return d
 
# Reduce column types among rows to find common denominator 
def reduceTypes(a,b):
    type_order = {'string':0, 'date':1, 'double':2, 'int':3, 'none':4}
    reduce_map = {'int': {0:'string', 1:'string',2:'double'}, 'double': {0:'string',1:'string'}, 'date': {0:'string'}}    
    d = a      
    for col, a_type in enumerate(a):
        #a_type = a[col]
        b_type = b[col]
        if a_type == 'none':
            d[col] = b_type
        elif b_type == 'none':
            d[col] = a_type
        else:
            order_a = type_order[a_type]
            order_b = type_order[b_type]
            if order_a == order_b:
                d[col] = a_type                
            elif order_a > order_b:
                d[col] = reduce_map[a_type][order_b]
            elif order_a < order_b:
                d[col] = reduce_map[b_type][order_a]           
    return d
        
def evaluateType(rdd_sql,parseDate):
    if parseDate:
        return rdd_sql.map(getRowType).reduce(reduceTypes)
    else:
        return rdd_sql.map(getRowTypeNoDate).reduce(reduceTypes)


		
 
