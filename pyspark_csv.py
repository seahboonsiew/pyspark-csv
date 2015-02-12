"""
PySpark-CSV

Convert CSV file into RDD using PySpark
Supported types: 
- Int
- Double
- String
- Datetime
- Accepts None, ? and '' as null values
"""
import csv
import dateutil.parser
from pyspark import sql

# Main function
# If columns not supplied, assume first row is the header
def csvToRDD(sqlCtx,rdd,columns=None):
    rdd_array = rdd.map(toRow)
    rdd_sql = rdd_array
    if columns is None:
        columns = rdd_array.first()
        rdd_sql = rdd_array.zipWithIndex().filter(lambda (r,i): i > 0).keys()
    zipped_rdd = rdd_sql.map(lambda r: zip(columns,r))
    column_types = evaluateType(zipped_rdd)
    return  sqlCtx.inferSchema(zipped_rdd.map(lambda r: (r,column_types)).map(toSqlRow))

# Find the common denominator schema of a SchemaRDD 
def checkSchemaRDD(rdd):
	columns = rdd.first().asDict().keys()
	return evaluateType(rdd.map(lambda r: (columns,r)))

	
# Actual conversion to sql.Row
def toSqlRow((row,col_types)):
    d = {} 
    for (col,data) in row:
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
    return sql.Row(**d)
	
# Type converter
def isNone(d):
    return (d is None or d == 'None' or d == '?' or d == '')

def toSeconds(TIME):
    h = int(TIME[0])
    m = int(TIME[1])
    s = int(TIME[2])
    return 3600*h+60*m+s

def toDate(d):
    return dateutil.parser.parse(d).date()

# Infer types for each row
def getRowType(row):
    d = {} 
    for (col,data) in row:
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

# Reduce column types among rows to find common denominator
def reduceTypes(a,b):
    d = {}
    type_order = {'string':0, 'date':1, 'double':2, 'int':3, 'none':4}
    reduce_map = {'int': {0:'string', 1:'string',2:'double'}, 'double': {0:'string',1:'string'}, 'date': {0:'string'}}
    for col in a:
        a_type = a[col]
        b_type = b[col]
        order_a = type_order[a_type]
        order_b = type_order[b_type]
        if a_type == 'none':
            d[col] = b_type
        elif b_type == 'none':
            d[col] = a_type
        elif order_a != order_b:
			if order_a > order_b:
				d[col] = reduce_map[a_type][order_b]
			else:
				d[col] = reduce_map[b_type][order_a]
        else:
            d[col] = a_type
    return d

def evaluateType(rdd_sql):
    return rdd_sql.map(getRowType).reduce(reduceTypes)
	
def toRow(line):
    for r in csv.reader([line.encode('utf-8')], delimiter=","):
        return r 
		

