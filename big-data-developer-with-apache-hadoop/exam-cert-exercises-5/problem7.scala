sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --password cloudera \
  --username root \
  --table orders \
  --target-dir /user/cloudera/problem4_ques7/input \
  --as-textfile \
  --delete-target-dir  \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

val orders = sc.textFile("/user/cloudera/problem4_ques7/input").map(line => line.split(",")).map(r => (r(0),r(1),r(2),r(3))).toDF
orders.repartition(1).write.orc("/user/cloudera/problem4_ques7/output")

$ hdfs dfs -ls /user/cloudera/problem4_ques7/output
$ hdfs dfs -text /user/cloudera/problem4_ques7/output/part-r-00000-82008b4c-1960-422c-8ac1-ef5ac79d1b58.orc | head -n 20