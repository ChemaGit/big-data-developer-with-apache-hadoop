from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

import ConfigParser as cp
import sys

props = cp.RawConfigParser()
props.read("src/main/resources/application.properties")
env = sys.argv[1]

spark = SparkSession. \
    builder. \
    master(props.get(env, 'execution.mode')). \
    appName("Daily Product Revenue"). \
    getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "2")

inputBaseDir = props.get(env, 'input.base.dir')
orders = spark.read.json(inputBaseDir + '/orders')
orderItems = spark.read.json(inputBaseDir + '/order_items')

dailyProductRevenue = orders. \
    where("order_status in ('CLOSED', 'COMPLETE')"). \
    join(orderItems, orders.order_id == orderItems.order_item_order_id). \
    groupBy(orders.order_date, orderItems.order_item_product_id). \
    agg(sf.sum('order_item_subtotal').alias('revenue'))
dailyProductRevenueSorted = dailyProductRevenue. \
    orderBy(dailyProductRevenue.order_date, dailyProductRevenue.revenue.desc())

outputBaseDir = props.get(env, 'output.base.dir')
dailyProductRevenueSorted.write.json(outputBaseDir + '/daily_product_revenue')