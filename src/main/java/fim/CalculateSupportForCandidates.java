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
import java.util.stream.Collectors;

public class CalculateSupportForCandidates extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(CalculateSupportForCandidates.class);

	public static class CandidateMapper extends Mapper<Object, Text, Text, IntWritable> {

		HashMap<String, List<String>> candidateKSet= new HashMap<>(); //Ck - candidate set for the current k
		// Ex: {(1, 2, 3), (1, 2, 4),(1, 2, 5), (1, 3, 8), (1, 3, 10)} => {(1, 2) => [3, 4, 5], (1, 3) => [8, 10]}
		int k; //current iteration of the item set generation

		/**
		 * In setup, we read the candidate lists under candidate folder and store them in the set to be
		 * checked in map phase.
		 */
		@Override
		public void setup(Context context){

			try {
				String key;
				String value;
				List<String> temp;
				Configuration conf = context.getConfiguration();

				k = Integer.parseInt(conf.get("K"));
				Path inputDirectory = new Path(conf.get("Candidates"));
				Path inputFolder = new Path("/" + inputDirectory.getParent().getName() + "/" + inputDirectory.getName() + "/");

				// Read candidateList from candidate folder
				FileSystem fs = FileSystem.get(URI.create("s3://cs6240santosh"), conf); // Pass the URI of bucket here
				FileStatus[] valFilePathList = fs.listStatus(inputFolder);

				for (final FileStatus fileEntry : valFilePathList) { // Get the list of files in the folder

					logger.info("Setup read: "+fileEntry.getPath().getName());
					System.out.println("Setup read: "+fileEntry.getPath().getName());

					if(fileEntry.getPath().getName().startsWith("part")){ // If the filename starts with Part
						BufferedReader rdr = new BufferedReader(new InputStreamReader(fs.open(fileEntry.getPath())));

						String line;
						while ((line = rdr.readLine()) != null) {  // Read lines for each record in the file
							int i = line.lastIndexOf(",");
							key=  line.substring(0, i);
							value= line.substring(i+1);

							//Storing new Key in HashMap
							if(candidateKSet.containsKey(key)){ 	//Checking if key already exists in HashMap
								temp = candidateKSet.get(key);
							}
							else{
								temp = new ArrayList<>();
							}
							temp.add(value);
							candidateKSet.put(key,temp); 	//Add the value to the hashmap
						}
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		/** Similar to the combination function in combination class
		 * But this takes care of out of memory exceptions.
		 * It uses the candidateKset to remove combinations in case of
		 * Out of memory exception
		* **/
		public void Combinations(Integer[] input, int index, int k, Set<List<Integer>> subArrays, List<Integer> leaf)
		{
			// invalid input
			if (input.length == 0 || k > input.length) {
				return;
			}

			// base case: combination size is `k`
			if (k == 0) {
				try{
				subArrays.add(new ArrayList<>(leaf));
				} catch (OutOfMemoryError e){ 	// Here we remove combinations that are not in the HashMap.
					//i.e. Apriori Gen did not consider.

					String leafString, initialCombination, followingCombination;
					int i;
					for(List<Integer> element: subArrays){
						leafString = element.stream().map(String::valueOf).collect(Collectors.joining(","));
						i= leafString.lastIndexOf(",");
						initialCombination=  leafString.substring(0, i);
						followingCombination= leafString.substring(i+1);
						try {
							if (candidateKSet.get(initialCombination).contains(followingCombination)) {
								continue;
							} else {
								subArrays.remove(element);
							}
						} catch (NullPointerException out){
							subArrays.remove(element);
						}
					}
					subArrays.add(new ArrayList<>(leaf));
				}
				return;
			}

			for (int itr = index; itr < input.length; itr++)
			{
				// add current element to the leaf and recur for next index
				leaf.add(input[itr]);
				Combinations(input, itr+ 1, k - 1, subArrays, leaf);
				leaf.remove(leaf.size() - 1);        // backtrack
			}
		}

		public Set<List<Integer>> findCombinations(Integer[] input, int k)
		{
			Set<List<Integer>> subArray = new HashSet<>();
			if(input.length == k){			//send the input back if input.size == k
				List<Integer> list = new ArrayList<>(input.length);

				for (int i: input) {
					list.add(i);
				}
				subArray.add(list);

			} else {
				Combinations(input, 0, k, subArray, new ArrayList<>());	 //calculate all the possible combination
			}
			return subArray;
		}

		/**
		 * In map phase, we get each transaction and generate the combination, then for each combination
		 * we emit the ones that are in the HashMap
		 */
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			Set<List<Integer>> combinations;
			String initialCombination, followingCombination;

			Configuration conf = context.getConfiguration();
			k = Integer.parseInt(conf.get("K"));

			List<Integer> transactionList = new ArrayList<>();
			final StringTokenizer itr = new StringTokenizer(value.toString(), ",");

			itr.nextToken();
			while (itr.hasMoreTokens()) {
				transactionList.add(Integer.parseInt(itr.nextToken()));
			}
			if(transactionList.size() >= k) {

				combinations = findCombinations(transactionList.toArray(new Integer[0]), k); //get all the combination
				for (List<Integer> c : combinations) {
					String items = c.toString();

					items = items.replace("[", "")
							.replace("]", "")
							.replace(" ", "");

					int i = items.lastIndexOf(",");
					initialCombination=  items.substring(0, i);
					followingCombination= items.substring(i+1);
					/* For items= "123,234,456"
					 * initialCombination= "123,234"
					 * followingCombination= "456"
					 * */

					try{
						if(initialCombination != followingCombination) { //sanity check
							if (candidateKSet.get(initialCombination).contains(followingCombination)) { // check if key exists and that it contains the followingCombination
								//pruning
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
		job.setJarByClass(CalculateSupportForCandidates.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		job.setMapperClass(CandidateMapper.class);
		job.setReducerClass(CandidateMapper.CandidateReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(hadoopArgs[0]));
		jobConf.set("Candidates", hadoopArgs[1]);
		jobConf.set("K", hadoopArgs[3]);
		jobConf.setInt("threshold", Integer.parseInt(hadoopArgs[4]));
		FileOutputFormat.setOutputPath(job, new Path(hadoopArgs[2]));

		// Configure the DistributedCache
		job.addCacheFile(new Path(hadoopArgs[0]).toUri());

		boolean returnResult = job.waitForCompletion(true);

		return returnResult ? 0 : 1;
	}
}