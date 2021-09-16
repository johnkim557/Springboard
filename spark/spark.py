if __name__ == '__main__':
    from pyspark.sql import SparkSession
    from pyspark import SparkContext as sc

    session = SparkSession.builder.master('local').appName('app').getOrCreate()

    sas_token = '?sv=2020-08-04&ss=bfqt&srt=co&sp=rwdlacuptfx&se=2021-09-13T19:04:32Z&st=2021-09-13T11:04:32Z&spr=https&sig=Cbk7pG9t7SFZnSRqZNcFxqkDB7oAzv3oFJREL5X30x4%3D'

    session.conf.set("fs.azure.sas.capstonecontainer.azcopyblobstorage.blob.core.windows.net",sas_token)

    from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DateType,TimestampType,DecimalType

    csv_schema = StructType([ 
        StructField("trade_date",DateType(),True),
        StructField("trade_dt",DateType(),True), \
        StructField("rec_type",StringType(),True), \
        StructField("symbol",StringType(),True), \
        StructField("event_tm", TimestampType(), True), \
        StructField("event_seq_nb", IntegerType(), True), \
        StructField("platform", StringType(), True),
        StructField("bid_pr", DecimalType(),True),
        StructField("bid_size", IntegerType(),True),
        StructField("ask_pr",DecimalType(),True),
        StructField("ask_size",IntegerType(),True),
        StructField("partition",StringType(),True)
    ])

    raw_csv = session.read.format("csv").option("path", "wasbs://capstonecontainer@azcopyblobstorage.blob.core.windows.net/part-00000-214fff0a-f408-466c-bb15-095cd8b648dc-c000.txt").schema(csv_schema).load()

    rdd_csv = raw_csv.rdd

    def parse_csv1(line):
        if line[2] == 'Q':
  
            trade_date = line[0]
            date_tm = line[1]
            rec_type = line[2]
            symbol = line[3]
            event_tm = line[4]
            event_seq_tm = line[5]
            platform = line[6]
            bid_price = line[7]
            bid_size = line[8]
            ask_price = line[9]
            ask_size = line[10]
            partition_value = line[11]
            return trade_date, date_tm, rec_type, symbol, event_tm, event_seq_tm,platform,bid_price,bid_size,ask_size,'Q'
        elif line[2] == 'T':
    
  
            trade_date = line[0]
            date_tm = line[1]
            rec_type = line[2]
            symbol = line[3]
            event_tm = line[4]
            event_seq_tm = line[5]
            platform = line[6]
            bid_price = line[7]
            bid_size = line[8]
            return trade_date, date_tm, rec_type, symbol, event_tm, event_seq_tm,platform,bid_price,'null','null','T'
        else:
            return 'null','null','null','null','null','null','null','null','null','null','B'


    parsed_csv = rdd_csv.map(lambda line : parse_csv1(line))

    parsed_csv.collect()

