from lib import ConfigReader


def convert_data(spark, env):
    conf = ConfigReader.get_app_config(env)
    zomato_file_path = conf["zomato.file.path"]
    zomato_csv = spark.read \
    .format('csv') \
    .option('header',True) \
    .option('inferSchema', True) \
    .load(zomato_file_path)

    zomato_csv = zomato_csv.withColumnRenamed('approx_cost(for two people)','approx_cost_for_two')
    zomato_csv = zomato_csv.withColumnRenamed('listed_in(type)','listed_in_type')
    zomato_csv = zomato_csv.withColumnRenamed('listed_in(city)','listed_in_city')
    
    write_to = conf["raw.file.path"]
    zomato_csv.write \
    .option("header", True) \
    .mode('overwrite') \
    .option('path',write_to) \
    .save()
