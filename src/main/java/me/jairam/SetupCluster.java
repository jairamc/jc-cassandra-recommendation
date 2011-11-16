package me.jairam;

import java.util.Arrays;

import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

public class SetupCluster {

	/**
	 * @param args
	 */
	public static void main(String[] args) 
	{
		if (args.length < 2)
		{
			printUsage();
			System.exit(1);
		}
		
		String clusterName = args[0];
		String connectionString = args[1];
		
		Cluster cluster = HFactory.getOrCreateCluster(clusterName, connectionString);
		
		ColumnFamilyDefinition documents = HFactory.createColumnFamilyDefinition("DocumentStore",                              
				"Documents", 
				ComparatorType.UTF8TYPE);

		ColumnFamilyDefinition users = HFactory.createColumnFamilyDefinition("DocumentStore",                              
				"Users", 
				ComparatorType.UTF8TYPE);
		
		ColumnFamilyDefinition authors = HFactory.createColumnFamilyDefinition("DocumentStore",                              
				"Authors", 
				ComparatorType.UTF8TYPE);
		
		ColumnFamilyDefinition suggestions = HFactory.createColumnFamilyDefinition("DocumentStore",                              
				"Suggestions", 
				ComparatorType.UTF8TYPE);

		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition("DocumentStore",                 
				ThriftKsDef.DEF_STRATEGY_CLASS,  
				1, 
				Arrays.asList(documents, users, authors, suggestions));
		
		//Add the schema to the cluster.
		//"true" as the second param means that Hector will block until all nodes see the change.
		cluster.addKeyspace(newKeyspace, true);

	}
	
	private static void printUsage()
	{
		System.out.println("java -cp <jar name> me.jairam.SetupCluster \"<cluster name>\" \"<connect location:port>\"");
	}

}
