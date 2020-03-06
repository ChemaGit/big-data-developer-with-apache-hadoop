# Question 89
````text
   Problem Scenario 29 : Please accomplish the following exercises using HDFS command line options.
   1. Create a directory in hdfs named hdfs_commands.
   2. Create a file in hdfs named data.txt in hdfs_commands.
   3. Now copy this data.txt file on local filesystem, however while copying file please make sure file properties are not changed e.g. file permissions.
   4. Now create a file in local directory named data_local.txt and move this file to hdfs in hdfs_commands directory.
   5. Create a file data_hdfs.txt in hdfs_commands directory and copy it to local file system.
   6. Create a file in local filesystem named file1.txt and put it to hdfs
````  
  
````properties  
// 1. Create a directory in hdfs named hdfs_commands.
$ hdfs dfs -mkdir /user/cloudera/hdfs_commands
// 2. Create a file in hdfs named data.txt in hdfs_commands.
$ hdfs dfs -touchz /user/cloudera/hdfs_commands/data.txt
// 3. Now copy this data.txt file on local filesystem, however while copying file please make sure file properties are not changed e.g. file permissions.
$ mkdir /home/cloudera/hdfs_commands
$ hdfs dfs -get -p /user/cloudera/hdfs_commands/data.txt /home/cloudera/hdfs_commands
$ ls -ltr /home/cloudera/hdfs_commands
// 4. Now create a file in local directory named data_local.txt and move this file to hdfs in hdfs_commands directory.
$ touch /home/cloudera/hdfs_commands/data_local.txt
$ hdfs dfs -put /home/cloudera/hdfs_commands/data_local.txt /user/cloudera/hdfs_commands
// 5. Create a file data_hdfs.txt in hdfs_commands directory and copy it to local file system.
$ hdfs dfs -touchz /user/cloudera/hdfs_commands/data_hdfs.txt
$ hdfs dfs -get /user/cloudera/hdfs_commands/data_hdfs.txt  /home/cloudera/hdfs_commands
// 6. Create a file in local filesystem named file1.txt and put it to hdfs
$ touch /home/cloudera/hdfs_commands/file1.txt
$ hdfs dfs -put /home/cloudera/hdfs_commands/file1.txt /user/cloudera/hdfs_commands
// Check local directory and hdfs directory
$ ls -ltr /home/cloudera/hdfs_commands
$ hdfs dfs -ls /user/cloudera/hdfs_commands
````