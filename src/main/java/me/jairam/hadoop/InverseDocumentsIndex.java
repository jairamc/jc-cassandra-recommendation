package me.jairam.hadoop;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InverseDocumentsIndex extends Configured implements Tool {

	private static final Logger logger = LoggerFactory.getLogger(InverseDocumentsIndex.class);
	
	private static String KEYSPACE = "DocumentStore";
	private static String OUTPUT_COLUMN_FAMILY = "DocumentInverse";
	private static String INPUT_COLUMN_FAMILY = "Users";

	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options
		ToolRunner.run(new Configuration(), new InverseDocumentsIndex(), args);
		System.exit(0);

	}

	public static class IndexMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, IntWritable>
	{
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(ByteBuffer key,
				SortedMap<ByteBuffer, IColumn> value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.map(key, value, context);
		}

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			sourceColumn = ByteBufferUtil.bytes(context.getConfiguration().get(CONF_COLUMN_NAME));
		}

		private ByteBuffer sourceColumn;


	}

	public static class IndexReducer extends Reducer<Text, IntWritable, ByteBuffer, List<Mutation>>
	{



	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "InverseDocumentsIndex");
		job.setJarByClass(InverseDocumentsIndex.class);
		job.setMapperClass(IndexMapper.class);


		job.setReducerClass(IndexReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(ByteBuffer.class);
		job.setOutputValueClass(List.class);

		job.setOutputFormatClass(ColumnFamilyOutputFormat.class);

		ConfigHelper.setOutputColumnFamily(job.getConfiguration(), KEYSPACE, OUTPUT_COLUMN_FAMILY);


		job.setInputFormatClass(ColumnFamilyInputFormat.class);


		ConfigHelper.setRpcPort(job.getConfiguration(), "9160");
		ConfigHelper.setInitialAddress(job.getConfiguration(), "localhost");
		ConfigHelper.setPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
		ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, INPUT_COLUMN_FAMILY);
		SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes("title")));
		ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);

		job.waitForCompletion(true);
	}
	return 0;
}




}
