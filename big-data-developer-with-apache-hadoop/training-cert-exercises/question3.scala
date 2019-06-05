/** Question 3
  * Problem Scenario 19 : You have been given following mysql database details as well as
  * other info.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Now accomplish following activities.
  * 1. Import departments table from mysql to hdfs as textfile in departments_text directory.
  * 2. Import departments table from mysql to hdfs as sequncefile in departments_sequence directory.
  * 3. Import departments table from mysql to hdfs as avro file in departments_avro directory.
  * 4. Import departments table from mysql to hdfs as parquet file in departments_parquet directory.
  */
// 1. Import departments table from mysql to hdfs as textfile in departments_text directory.
sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
  --username root \
  --password cloudera \
  --table departments \
  --as-textfile \
  --delete-target-dir \
  --target-dir /user/cloudera/question3/departments_text \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

$ hdfs dfs -ls /user/cloudera/question3/departments_text
$ hdfs dfs -cat /user/cloudera/question3/departments_text/part* | tail -n 20

// 2. Import departments table from mysql to hdfs as sequncefile in departments_sequence directory.
sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
  --username root \
  --password cloudera \
  --table departments \
  --as-sequencefile \
  --delete-target-dir \
  --target-dir /user/cloudera/question3/departments_sequence \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

$ hdfs dfs -ls /user/cloudera/question3/departments_sequence
$ hdfs dfs -cat /user/cloudera/question3/departments_sequence/part-m-00000 | tail -n 20

// 3. Import departments table from mysql to hdfs as avro file in departments_avro directory.
sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
  --username root \
  --password cloudera \
  --table departments \
  --as-avrodatafile \
  --delete-target-dir \
  --target-dir /user/cloudera/question3/departments_avro \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

$ hdfs dfs -ls /user/cloudera/question3/departments_avro
$ avro-tools getmeta hdfs://quickstart.cloudera/user/cloudera/question3/departments_avro/part-m-00000.avro
$ avro-tools tojson hdfs://quickstart.cloudera/user/cloudera/question3/departments_avro/part-m-00000.avro

// 4. Import departments table from mysql to hdfs as parquet file in departments_parquet directory.
sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
  --username root \
  --password cloudera \
  --table departments \
  --as-parquetfile \
  --delete-target-dir \
  --target-dir /user/cloudera/question3/departments_parquet \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

$ hdfs dfs -ls /user/cloudera/question3/departments_parquet
$ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/question3/departments_parquet/645d8bc2-c7fb-4d66-ba9c-a03d1a52b06f.parquet
$ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/question3/departments_parquet/645d8bc2-c7fb-4d66-ba9c-a03d1a52b06f.parquet