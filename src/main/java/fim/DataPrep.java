package fim;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

public class DataPrep extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(DataPrep.class);

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		// Map gets the date, customer ID and product from the transaction and emits with the customer and
		// date grouped up, with product as the value.
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			final StringTokenizer itr = new StringTokenizer(value.toString(),",");
			String date = itr.nextToken(); // Get Date
			String customerID = itr.nextToken(); // Get Customer ID
			String article = itr.nextToken(); // Get Item
			String group_key = date + "," + customerID; // Group date and CustomerID, to get the transaction for the day
			if(!article.equals("article_id")) {
				context.write(new Text(group_key), new Text(article));
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, NullWritable, Text> {
		private final NullWritable nw = NullWritable.get();

		// Reduce gets all the items for a transaction and gets them in a list and writes them to the file
		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			List<Integer> item_list=new ArrayList<Integer>();
			for (final Text val : values) {
				int item = Integer.parseInt(val.toString());
				item_list.add(item); // Add the item to the list
			}
			Collections.sort(item_list); // Sort the list
			String items = item_list.stream().map(n-> String.valueOf(n)).collect(Collectors.joining(",","","")); // Convert the list to a string
			context.write(nw,new Text(items));
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(DataPrep.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		// Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		// ================
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(IntSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new DataPrep(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}