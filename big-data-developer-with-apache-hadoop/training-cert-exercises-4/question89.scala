/** Question 89
 * Problem Scenario 29 : Please accomplish the following exercises using HDFS command line options.
 * 1. Create a directory in hdfs named hdfs_commands.
 * 2. Create a file in hdfs named data.txt in hdfs_commands.
 * 3. Now copy this data.txt file on local filesystem, however while copying file please make sure file properties are not changed e.g. file permissions.
 * 4. Now create a file in local directory named data_local.txt and move this file to hdfs in hdfs_commands directory.
 * 5. Create a file data_hdfs.txt in hdfs_commands directory and copy it to local file system.
 * 6. Create a file in local filesystem named file1.txt and put it to hdfs
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : Create directory 
$ hdfs dfs -mkdir hdfs_commands 

//Step 2 : Create a file in hdfs named data.txt in hdfs_commands. 
$ hdfs dfs -touchz hdfs_commands/data.txt 

//Step 3 : Now copy this data.txt file on local filesystem, however while copying file please make sure file properties are not changed e.g. file permissions. 
$ hdfs dfs -copyToLocal -p hdfs_commands/data.txt /home/cloudera/Desktop/HadoopExam 

//Step 4 : Now create a file in local directory named data_local.txt and move this file to hdfs in hdfs_commands directory. 
$ touch data_local.txt 
$ hdfs dfs -moveFromLocal /home/cloudera/Desktop/HadoopExam/data_local.txt hdfs_commands/ 

//Step 5 : Create a file data_hdfs.txt in hdfs_commands directory and copy it to local file system. 
$ hdfs dfs -touchz hdfs_commands/data_hdfs.txt 
$ hdfs dfs -get hdfs_commands/data_hdfs.txt /home/cloudera/Desktop/HadoopExam/ 

//Step 6 : Create a file in local filesystem named filel .txt and put it to hdfs 
$ touch file1.txt 
$ hdfs dfs -put/home/cloudera/Desktop/HadoopExam/file1.txt hdfs_commands/