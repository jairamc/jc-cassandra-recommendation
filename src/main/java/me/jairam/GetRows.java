package me.jairam;

import java.util.List;

import com.scriptandscroll.adt.*;

public class GetRows {

	public static void main(String[] args)
	{
		Keyspace ks = new Keyspace("Test Cluster", "DocumentStore", "localhost:9160");
		
		String columnFamily = args[0];
		
		ColumnFamily cf = new ColumnFamily(ks, columnFamily);
		
		List<Row> rows = cf.getRows("", "", "", "", false, 20, 5);
		
		if (columnFamily.equalsIgnoreCase("Documents"))
		{
			for(Row row: rows)
			{
				System.out.println(row.getKey() + ":" + row.getColumnValue("title"));
				System.out.println(row.getKey() + ":" + row.getColumnValue("authors"));
			}
		}
		else if (columnFamily.equalsIgnoreCase("Authors"))
		{
			for(Row row: rows)
			{
				System.out.println(row.getKey() + ":" + row.getColumnValue("author"));
				System.out.println(row.getKey() + ":" + row.getColumnValue("title"));
			}
		}
		else if (columnFamily.equalsIgnoreCase("Users"))
		{
			for(Row row: rows)
			{
				System.out.println(row.getKey() + ":" + row.getColumnValue("name"));
				System.out.println(row.getKey() + ":" + row.getColumnValue("books"));
			}
		}
		else if(columnFamily.equalsIgnoreCase("DocumentInverse"))
		{
			for(Row row: rows)
			{
				System.out.println(row.getKey() + ":" + row.getColumnValue("users"));
			}
		}
		
	}
}
