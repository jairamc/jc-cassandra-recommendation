package me.jairam;

import com.scriptandscroll.adt.*;

public class MyCass {

	
	
	public static void main(String[] args)
	{
		Keyspace ks = new Keyspace("Test-Cluster", "checking", "localhost:9160");
		ColumnFamily cf = new ColumnFamily(ks, "position");
		
		Column lat = new Column("lat", "1234");
		Column lon = new Column("lon", "5678");
		
		
		cf.putColumn("Row-2", lat);
		cf.putColumn("Row-2", lon);
		
		Column latFromCass = cf.getColumn("Row-2", "lat");
		
		System.out.println(latFromCass.getName() + ":" + latFromCass.getValue());
		
	}
}
