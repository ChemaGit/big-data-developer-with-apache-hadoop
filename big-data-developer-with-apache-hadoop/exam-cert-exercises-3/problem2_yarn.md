# Question 2: Correct
````text
Which three basic configuration parameters must you set to migrate your cluster from MapReduce 1 (MRv1) to MapReduce V2 (MRv2)?
Configure the nodeNanager to enable MapReduce services on YARN by setting the following property in yarn-site.xml
````

````xml
<name>yarn.nodemanager.hostname</name>
  <value>your_nodeManager_shuffle</value>
````