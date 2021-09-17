from pyspark.sql import SparkSession

#spark = SparkSession.builder.appName('spark').getOrCreate()
from pyspark.context import SparkContext
from pyspark import SparkConf
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))


#change location
#raw_rdd = sc.textFile("/Users/macbookpro/Desktop/springboard/Spark_Unit_20/data.csv")

raw_rdd.collect()

def extract_vin_key_value(line):
  '''
  function must output a Pair Rdd which is a tuple type
  Example: (Germany<-key, 1 <- value)
  '''
  
  vin_key_dict = {}
  
  record = line.split(",")
  #record[1] = incident type
  #record[2] = vin number
  #record[3] = make
  #record[4] = year
  return record[2],(record[3]+'-'+record[5],record[1])


vin_kv = raw_rdd.map(lambda line: extract_vin_key_value(line))

vin_kv.collect()

def populate_make(record):
    master_info = (record)
    return master_info

enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))

enhance_make.collect()

make_kv = enhance_make.map(lambda x: (x,1))

filtered_rdd = make_kv.filter(lambda a: a[0][0]!='-').map(lambda x: (x[0][0],x[1]))

from operator import add
reduced_rdd = filtered_rdd.reduceByKey(add)

reduced_rdd.collect()