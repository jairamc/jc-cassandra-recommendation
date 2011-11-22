This is a project I created for a presentation I did @cassandralondon on Hadoop integration in Cassandra. 

If you would like to try out the code just follow the steps - 

Step 1 : Clone/fork the project

Step 2 : Clone/fork (and see instructions) http://github.com/steeve/brisk 
  Basically, you need a machine that is running both Cassandra and Hadoop for the Hadoop/Pig jobs. In case you want to try the Hive examples, you _will_ need Brisk. 
  Make sure you have these running before proceeding


Step 3 : Extract the SampleDocuments.tar.gz and SampleUserNames.tar.gz file

Step 4 : mvn clean package

Step 5 : java -cp <jar name from Step 3 in target folder> me.jairam.SetupCluster

Step 6 : java -cp <jar name from Step 3 in target folder> me.jairam.SetupSampleData <location to SampleDocuments> <location to SampleUserNames>

This wil have you cluster setup. 

Files - 

me.jairam.hadoop.InverseDocumentsIndex.java - Populates a new column family in Cassandra 


You are free to look into the code. Any feedback appreciated. 

The two main repos that you might want to look into are - 

http://github.com/rantav/hector
http://github.com/zcourts/cassandra-hector-wrapper
