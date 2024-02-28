from pyspark.sql.functions import *
from lib import ConfigReader


def clean_data(raw_data,env):
    conf = ConfigReader.get_app_config(env)
    clean_file_path = conf["cleaned.file.path"]
    print('Drop url, location, dish_liked, cuisines, review_list, menu_item, listed_in_type, listed_in_city')
    
    dropped_cols_df =raw_data.drop('url', 'location', 'dish_liked', 'cuisines', 'review_list', 'menu_item', 'listed_in_type', 'listed_in_city')
    
    print("Replace rows that don't have proper resuturant names to 'other'")
    
    name_lookup = ['Kiosk','Food Court, Quick Bites', 'Bakery, Cafe','Cafe, Bakery', 'null','Mess', 'Bakery, Dessert Parlor','Cafe, Casual Dining','Sweet Shop, Quick Bites', 'Dessert Parlor, Cafe',
              'Quick Bites', 'Casual Dining', 'Cafe', 'Delivery', 'Dessert Parlor', 'Casual Dining, Bar', 'Takeaway, Delivery', 'Bakery', 'Bar', 'Lounge', 
               'Food Court', 'Pub', 'Casual Dining, Cafe', 'Beverage Shop', 'Fine Dining', 'Sweet Shop', 'Bar, Casual Dining' ,'Pub, Casual Dining' ,'Bakery, Quick Bites', 'Casual Dining, Microbrewery'
             'Dessert Parlor, Bakery', 'Cafe, Dessert Parlor', 'Pub, Microbrewery']
    name_modified_df = dropped_cols_df.withColumn('name', when(~col('name').isin(name_lookup),col('name')).otherwise('other'))
    name_dropped = name_modified_df.filter(col("name") != "other")

    print("Replace all the unneccessary values in online_order, book_table column with no")
    
    order_table_modified_df = name_dropped.withColumn('online_order', when(col('online_order').isin(['Yes', 'No']), col('online_order')).otherwise('No'))
    book_col_modified_df = name_dropped.withColumn('book_table', when(col('book_table').isin(['Yes', 'No']), col('book_table')).otherwise('No'))

    print("Replace rows with null to 0.0 then convert rate into float round the value to 1 decimal")
    
    rate_modified_df = book_col_modified_df.withColumn("rate", when(col("rate").contains("/5"), col("rate")).otherwise("0"))
    rate_float_converted = rate_modified_df.withColumn("rate", regexp_replace(col("rate"), "/5", "").cast('float'))

    print("Replace all the unneccessary values in votes column with 0")
    
    votes_modified_df = rate_float_converted.withColumn("votes", regexp_extract(col("votes"), r"(\d+)", 1).cast("int"))
    votes_modified_df = votes_modified_df.fillna({'votes':0})
    
    print("Replace rows where there's no phone number and if there's more than one then covert this col into array type")
   
    phone_num_changed = votes_modified_df.withColumn('phone', when(length(col('phone')) < 10, 0).otherwise(col('phone')))

    print("replace all the bad data in rest_type with eateries")

    rest_type_updated = phone_num_changed.fillna({'rest_type':'eateries'})

    print("convert all bad data and null vlaues to 0 in approx_cost_for_two")
    cost_updated_df = rest_type_updated.withColumn("approx_cost_for_two", regexp_extract(col("approx_cost_for_two"), r"(\d+)", 1).cast("int"))
    cost_updated_df = cost_updated_df.fillna({'approx_cost_for_two':0})

    print("replace null values in review list to 'no rating'")
    review_list_modifies = cost_updated_df.withColumn("reviews_list", when(col("reviews_list").contains("Rated"), col("reviews_list")).otherwise("no rating"))

    print("Write the clean data into a different location")
    return review_list_modifies.write \
    .option("header", True) \
    .mode('overwrite') \
    .option('path',clean_file_path) \
    .save()