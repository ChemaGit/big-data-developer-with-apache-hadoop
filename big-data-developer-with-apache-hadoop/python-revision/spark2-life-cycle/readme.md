# Development life cycle of Spark 2 applications using Python (using Pycharm)

	- As part of this session we will see end to end development life cycle to build Spark 2 applications using Python as programming languages. 
	- We will be using Pycharm IDE to build application.
    		- Define Problem Statement
    		- Develop using Pycharm
    		- Configure necessary dependencies
    		- Externalize Properties
    		- Develop application using Spark Data Frames
    		- Validate locally
    		- Get it ready to run on the EMR cluster

# Problem Statement
	- Let us define problem statement and see the steps to develop the application.

    		- Get Daily Product Revenue (for completed and closed orders)
    		- We need orders and order_items to come up with the solution
    		- Final output need to be sorted in ascending order by date and descending order by revenue
    		- Save order_date, product_id and revenue in the form of JSON

# Create Project and Dependencies
	- Let us see how to create project for pyspark application.

    		- Click on New Project
    		- Choose appropriate python interpreter
    		- Give name to the project and click create
    		- Go to PyCharm Preferences and then to Project Structure
    		- Add python and py4j to content root

# Externalize Properties
	- Let us see the relevance of externalizing properties.

    		- We need to control the way application run in different environments such as dev, test, prod etc
    		- We need to define input path and output path
    		- These things can be either passed as arguments or externalize properties
    		- In typical applications we will have tens of properties and passing all those arguments can be unmanageable
    		- It is better to externalize properties
    		- There is package called ConfigParser which come as one of the Python core modules provide necessary APIs to load properties file and give us properties as map like object.
    		- Create properties file with dev and prod categories (under src/main/resources)

````properties
[dev]
execution.mode = local
input.base.dir = /Users/itversity/Research/data/retail_db_json
output.base.dir = /Users/itversity/Research/data/bdclouddemo/pyspark

[prod]
execution.mode = yarn-client
input.base.dir = s3://itversitydata/retail_db_json
output.base.dir = s3://itversitydata/bdclouddemo/pyspark
````

# Develop Application
	- Let us develop the application to get daily product revenue for completed and closed orders.

    		- Write code to load application.properties
    		- Create SparkSession object
    		- Read data from JSON files and create data frames out of them
    		- Use APIs such as select, where, groupBy, agg with sum, orderBy etc to process data
    		- Save final output in the form of JSON

````python
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

import ConfigParser as cp
import sys

props = cp.RawConfigParser()
props.read("src/main/resources/application.properties")
env = sys.argv[1]

spark = SparkSession.\
    builder.\
    master(props.get(env, 'execution.mode')).\
    appName("Daily Product Revenue").\
    getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "2")

inputBaseDir = props.get(env, 'input.base.dir')
orders = spark.read.json(inputBaseDir + '/orders')
orderItems = spark.read.json(inputBaseDir + '/order_items')

dailyProductRevenue = orders.\
    where("order_status in ('CLOSED', 'COMPLETE')").\
    join(orderItems, orders.order_id == orderItems.order_item_order_id).\
    groupBy(orders.order_date, orderItems.order_item_product_id).\
    agg(sf.sum('order_item_subtotal').alias('revenue'))
dailyProductRevenueSorted = dailyProductRevenue.\
    orderBy(dailyProductRevenue.order_date, dailyProductRevenue.revenue.desc())

outputBaseDir = props.get(env, 'output.base.dir')
dailyProductRevenueSorted.write.json(outputBaseDir + '/daily_product_revenue')
````

# Validate Locally

	- Make sure you run the job locally.

    		- Right click and run the application. It will fail for first time, but will create application in run wizard
    		- Go to edit configurations in run wizard and pass dev as first argument
    		- Now right click again and run
    		- If it return with exit code 0, the run is successful
    		- Now go to the output directory to validate that data is generated properly.




