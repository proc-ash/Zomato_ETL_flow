from lib import ConfigReader
#defining customers schema
def get_schema():
    zomato_schema = ('''address string,name string, online_order string, book_table string, rate float,
                      votes int, phone string, rest_type string, approx_cost_for_two int,
                      reviews_list string''')
    return zomato_schema

# creating customers dataframe
def read_clean_data(spark,env):
    conf = ConfigReader.get_app_config(env)
    clean_file_path = conf["cleaned.file.path"]
    return spark.read \
    .schema(get_schema()) \
    .load(clean_file_path)

def clean_raw_data(spark,env):
    conf = ConfigReader.get_app_config(env)
    raw_file_path = conf["raw.file.path"]
    return spark.read \
    .option('header',True) \
    .option('inferSchema',True) \
    .load(raw_file_path)