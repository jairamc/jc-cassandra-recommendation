package me.jairam.hadoop;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.StringTokenizer;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONArray;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InverseDocumentsIndex extends Configured implements Tool {

	private static final Logger logger = LoggerFactory.getLogger(InverseDocumentsIndex.class);

	private static String KEYSPACE = "DocumentStore";
	private static String OUTPUT_COLUMN_FAMILY = "DocumentInverse";
	private static String INPUT_COLUMN_FAMILY = "Users";
	private static String INPUT_COLUMN = "books";

	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options
		ToolRunner.run(new Configuration(), new InverseDocumentsIndex(), args);
		System.exit(0);

	}

	public static class IndexMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, Text>
	{
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(ByteBuffer key,
				SortedMap<ByteBuffer, IColumn> columns, Context context)
						throws IOException, InterruptedException 
		{
			IColumn column = columns.get(ByteBufferUtil.bytes(INPUT_COLUMN));
			if (column == null)
				return;
			String value = ByteBufferUtil.string(column.value());
			logger.info("read " + ByteBufferUtil.string(key) + ":" + value + " from " + context.getInputSplit());

			JSONArray titles;
			try 
			{
				titles = new JSONArray(value);
				Text user = new Text(key.array());
				for(int i = 0; i < titles.length(); i++)
				{
					context.write(new Text(titles.getString(i)), user);
				}
			}
			catch (JSONException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static class IndexReducer extends Reducer<Text, Text, ByteBuffer, List<Mutation>>
	{

		public void reduce(Text title, Iterable<Text> users, Context context) throws IOException, InterruptedException
		{
			JSONArray userList = new JSONArray();
			for(Text user: users)
			{
				userList.put(user.toString());
			}
			context.write(ByteBufferUtil.bytes(title.toString()), Collections.singletonList(getMutation("users", userList.toString())));
		}

		private static Mutation getMutation(String colName, String value)
		{
			Column c = new Column();
			c.setName(colName.getBytes());
			c.setValue(ByteBufferUtil.bytes(value));
			c.setTimestamp(System.currentTimeMillis());

			Mutation m = new Mutation();
			m.setColumn_or_supercolumn(new ColumnOrSuperColumn());
			m.column_or_supercolumn.setColumn(c);
			return m;
		}

	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "InverseDocumentsIndex");
		job.setJarByClass(InverseDocumentsIndex.class);

		job.setMapperClass(IndexMapper.class);
		job.setReducerClass(IndexReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(ByteBuffer.class);
		job.setOutputValueClass(List.class);

		job.setInputFormatClass(ColumnFamilyInputFormat.class);
		job.setOutputFormatClass(ColumnFamilyOutputFormat.class);

		ConfigHelper.setOutputColumnFamily(job.getConfiguration(), KEYSPACE, OUTPUT_COLUMN_FAMILY);

		ConfigHelper.setRpcPort(job.getConfiguration(), "9160");
		ConfigHelper.setInitialAddress(job.getConfiguration(), "localhost");
		ConfigHelper.setPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
		ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, INPUT_COLUMN_FAMILY);
		SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes(INPUT_COLUMN)));
		ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);

		job.waitForCompletion(true);

		return 0;
	}
}