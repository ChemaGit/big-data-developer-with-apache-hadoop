/*
Question 6: Correct
PreRequiste:
[PreRequiste would not be there in final exam]

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table orders \
--fields-terminated-by "\t" \
--target-dir /user/cloudera/practice3/problem3/orders

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table order_items \
--fields-terminated-by "\t" \
--target-dir /user/cloudera/practice3/problem3/order_items

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table customers \
--fields-terminated-by "\t" \
--target-dir /user/cloudera/practice3/problem3/customers



Instructions
Get count of customers in each city who have placed order of amount more than 100 and whose order status is not PENDING.

Input files are tab delimeted files placed at below HDFS location:
/user/cloudera/practice3/problem3/customers
/user/cloudera/practice3/problem3/orders
/user/cloudera/practice3/problem3/order_items

Schema for customers File
Customer_id,customer_fname,customer_lname,customer_email,customer_password,customer_street,customer_city,customer_state,customer_zipcode

Schema for Orders File
Order_id,order_date,order_customer_id,order_status

Schema for Order_Items File
Order_item_id,Order_item_order_id,order_item_product_id,Order_item_quantity,Order_item_subtotal,Order_item_product_price

Output Requirements:
Output should be placed in below HDFS Location
/user/cloudera/practice3/problem3/joinResults

Output file should be tab separated file with
[Providing the solution here only because answer is too long to put in choices.You will not be provided with any answer choice in actual exam.Below answer is just provided to guide you]
*/
sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --password cloudera \
  --username root \
  --table orders \
  --fields-terminated-by "\t" \
  --delete-target-dir \
  --target-dir /user/cloudera/practice3/problem3/orders \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --password cloudera \
  --username root \
  --table order_items \
  --fields-terminated-by "\t" \
  --delete-target-dir \
  --target-dir /user/cloudera/practice3/problem3/order_items \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --password cloudera \
  --username root \
  --table customers \
  --fields-terminated-by "\t" \
  --delete-target-dir \
  --target-dir /user/cloudera/practice3/problem3/customers \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

val orders = sc.textFile("/user/cloudera/practice3/problem3/orders").map(line => line.split('\t')).map(r => (r(0).toInt,r(1),r(2).toInt,r(3))).toDF("order_id","order_date","order_customer_id","order_status")
val orderItems = sc.textFile("/user/cloudera/practice3/problem3/order_items").map(line => line.split('\t')).map(r => (r(0).toInt,r(1).toInt,r(2).toInt,r(3).toInt,r(4).toFloat,r(5).toFloat)).toDF("item_id","item_order_id","order_item_product_id","item_quantity","item_subtotal","item_product_price")
val customers = sc.textFile("/user/cloudera/practice3/problem3/customers").map(line => line.split('\t')).map(r => (r(0).toInt,r(1),r(2),r(3),r(4),r(5),r(6),r(7),r(8))).toDF("customer_id","customer_fname","customer_lname","customer_email","customer_password","customer_street","customer_city","customer_state","customer_zipcode")

orders.registerTempTable("o")
orderItems.registerTempTable("oi")
customers.registerTempTable("c")

val joined = sqlContext.sql("""SELECT customer_city,customer_id,item_order_id, round(sum(item_subtotal),2) as total_revenue FROM c JOIN o ON(c.customer_id = o.order_customer_id) JOIN oi ON(o.order_id = oi.item_order_id) GROUP BY customer_city,customer_id,item_order_id""")

joined.registerTempTable("j")

val result = sqlContext.sql("""SELECT customer_city, count(customer_id) as total_customers from j where total_revenue > 100 group by customer_city""")

result.rdd.map(r => r.mkString("\t")).repartition(1).saveAsTextFile("/user/cloudera/practice3/problem3/joinResults")

$ hdfs dfs -ls /user/cloudera/practice3/problem3/joinResults
$ hdfs dfs -cat /user/cloudera/practice3/problem3/joinResults/p* | head -n 50