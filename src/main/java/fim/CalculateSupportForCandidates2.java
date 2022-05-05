package fim;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.util.*;

public class CalculateSupportForCandidates2 extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(CalculateSupportForCandidates2.class);

	public static class CandidateMapper extends Mapper<Object, Text, Text, IntWritable> {

		HashMap<Integer, Integer> candidateKSet= new HashMap<>(); //Ck - candidate set for the current k
		int k; //current iteration of the item set generation

		/**
		 * In setup, we read the output of the L1. The idea is that for a combination of k=2 to be valid. Both the items
		 * must have a support > threshold (min_support). Hence, we do not need to calculate candidate list.
		 * Because all the items in L1 will have a one to all combinations.
		 */
		@Override
		public void setup(Context context){

			try {
				String key; //to set HashMap
				Configuration conf = context.getConfiguration();

				k = Integer.parseInt(conf.get("K"));
				Path inputDirectory = new Path(conf.get("L1")); //path to L1

				//Path parentInputDirectory = inputDirectory.getParent().getParent();
				Path inputFolder = new Path("/" + inputDirectory.getParent().getName() + "/" + inputDirectory.getName() + "/"); //to generate the remaining part of the uri

//				 Read candidateList from candidate folder
				FileSystem fs = FileSystem.get(URI.create("s3://cs6240santosh"), conf); // Pass the URI of bucket here
				FileStatus[] valFilePathList = fs.listStatus(inputFolder);  //List of all the files in that folder (all part-r-****) files.

				for (final FileStatus fileEntry : valFilePathList) {
					if (fileEntry.getPath().getName().startsWith("part")) { //to not read _SUCCESS file

						logger.info("The path: "+ fileEntry.getPath().getName());

						BufferedReader rdr = new BufferedReader(new InputStreamReader(fs.open(fileEntry.getPath())));
						String line;

						while ((line = rdr.readLine()) != null) { // Read lines for each record in the file
							key=  line.split(",")[0];     //set key as item (product it)
							candidateKSet.put(Integer.parseInt(key),0);  //put the key in HashMap
						}
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		/**
		 * In map phase, we get each transaction and generate the combination, then for each combination
		 * we emit the ones of which both the elements are present in the HashMap.
		 */
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			Combination combination = new Combination();
			Set<List<Integer>> combinations;
			int item1, item2;

			Configuration conf = context.getConfiguration();
			k = Integer.parseInt(conf.get("K"));

			List<Integer> transactionList = new ArrayList<>();
			final StringTokenizer itr = new StringTokenizer(value.toString(), ",");

			itr.nextToken();   // skip the transaction id
			while (itr.hasMoreTokens()) {
				transactionList.add(Integer.parseInt(itr.nextToken())); //add all the products to a list
			}
			if(transactionList.size() >= k) {

				combinations = combination.findCombinations(transactionList.toArray(new Integer[0]), k); //returns all the combinations of size k
				for (List<Integer> c : combinations) {
					String items = c.toString();

					items = items.replace("[", "")
							.replace("]", "")
							.replace(" ", "");


					item1=  Integer.parseInt(items.split(",")[0]);
					item2= Integer.parseInt(items.split(",")[1]);

					try{
						if(item1 != item2) { //Sanity check
							if (candidateKSet.containsKey(item1) && candidateKSet.containsKey(item2)) { // check if both the items are in the HashMap
								// Pruning
								context.write(new Text(items), new IntWritable(1));
							}
						}
					} catch (NullPointerException ignored){
					}
				}
			}
		}

		/**
		 * In reduce phase we emit the result that is above threshold.
		 */
		public static class CandidateReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
			private final IntWritable result = new IntWritable();

			@Override
			public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
				Configuration conf= context.getConfiguration();
				int sum = 0;
				int threshold = conf.getInt("threshold", 100);

				for (final IntWritable val : values) {
					sum += val.get();
				}

				if (sum >= threshold) {
					result.set(sum);
					context.write(key, result);
				}
			}
		}
	}

	@Override
	public int run(final String[] hadoopArgs) throws Exception {
		final Configuration conf = getConf();

		final Job job = Job.getInstance(conf, "CalculateSupportForCandidates");
		job.setJarByClass(CalculateSupportForCandidates2.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		job.setMapperClass(CandidateMapper.class);
		job.setReducerClass(CandidateMapper.CandidateReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(hadoopArgs[0]));
		jobConf.set("L1", hadoopArgs[1]);
		jobConf.set("K", hadoopArgs[3]);
		jobConf.setInt("threshold", Integer.parseInt(hadoopArgs[4]));
		FileOutputFormat.setOutputPath(job, new Path(hadoopArgs[2]));

		// Configure the DistributedCache
		job.addCacheFile(new Path(hadoopArgs[0]).toUri());

		boolean returnResult = job.waitForCompletion(true);

		return returnResult ? 0 : 1;
	}
}