from pyspark.sql.functions import *

def analyse_data(zomato_data):
    zomato_data.show()

    zomato_data.createOrReplaceTempView('zomato')

    print("Total data count")
    zomato_data.count()

    print("address wise approx_cost")
    spark.sql("select address, approx_cost_for_two from zomato order by approx_cost_for_two desc").show()

    print("Resturant with maximum vote")
    spark.sql("select name, online_order, book_table, votes from zomato order by votes desc limit 1").show(truncate = False)

    print("Resturant with maximum rating")
    spark.sql("select * from zomato order by rate desc limit 1").show()