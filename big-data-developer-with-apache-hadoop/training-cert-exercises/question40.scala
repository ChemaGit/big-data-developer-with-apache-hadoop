/** Question 40
  * Problem Scenario 8 : You have been given following mysql database details as well as other info.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Please accomplish following.
  * 1. Import joined result of orders and order_items table join on orders.order_id = order_items.order_item_order_id.
  * 2. Also make sure each tables file is partitioned in 2 files e.g. part-00000, part-00002
  * 3. Also make sure you use order_id columns for sqoop to use for boundary conditions.
  */
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --query "SELECT orders.*,order_items.* FROM orders JOIN order_items ON(orders.order_id = order_items.order_item_order_id) AND \$CONDITIONS" \
  --split-by "orders.order_id" \
  --boundary-query "select min(orders.order_id),max(orders.order_id) from orders" \
  --as-textfile \
  --delete-target-dir \
  --target-dir /user/cloudera/question40/joined \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 2

$ hdfs dfs -ls /user/cloudera/question40/joined
$ hdfs dfs -cat /user/cloudera/question40/joined/part* | head -n 50