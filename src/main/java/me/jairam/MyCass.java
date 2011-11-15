package me.jairam;

import java.util.List;

import com.scriptandscroll.adt.*;

public class MyCass {

	
	
	public static void main(String[] args)
	{
		Keyspace ks = new Keyspace("Test Cluster", "DocumentStore", "localhost:9160");
		ColumnFamily cf = new ColumnFamily(ks, "Documents");
		
		List<Row> rows = cf.getRows("", "", "", "", false, 20, 5);
		
		for(Row row: rows)
		{
			System.out.println(row.getKey() + ":" + row.getColumnValue("title"));
			System.out.println(row.getKey() + ":" + row.getColumnValue("authors"));
		}
		
		
	}
}
