package fim;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;


public class AprioriGen extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(AprioriGen.class);

	public static class TokenizerMapper extends Mapper<Object, Text, NullWritable, Text> {
		private final NullWritable nw = NullWritable.get();
		final private  HashMap<String, List<String>> prev_itemset = new HashMap<>(); // To store Lk-1 for lookup
		int k; // Current iteration/previous itemset length

		@Override
		public void setup(Context context){
			try {

				Configuration conf = context.getConfiguration();

				k = Integer.parseInt(conf.get("K")); // Get the K value
				String prevFolder = conf.get("Previous Itemset"); // Get the folder name for the Lk-1 itemset

				Path inputDirectory = new Path(conf.get("Previous Itemset")); // Get the folder name for the Lk-1 itemset
				//Path parentInputDirectory = inputDirectory.getParent().getParent();
				Path inputFolder = new Path("/" + inputDirectory.getParent().getName() + "/" + inputDirectory.getName() + "/");

//				// Read candidateList from candidate folder
				FileSystem fs = FileSystem.get(URI.create("s3://cs6240santosh"), conf); // Pass the URI of bucket here
				FileStatus[] valFilePathList = fs.listStatus(inputFolder);

//				final File folder = new File(prevFolder);
				for (final FileStatus fileEntry : valFilePathList) { // Get the list of files in the folder

//					logger.info("Setup read: "+fileEntry.getName());

					if(fileEntry.getPath().getName().startsWith("part")){ // If the filename starts with Part
						BufferedReader rdr = new BufferedReader(new InputStreamReader(fs.open(fileEntry.getPath())));
						// Read lines for each record in the file
//						BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(prevFolder + "/" + fileEntry.getName()))); // Read lines for each record in the file
						String line;
						while ((line = rdr.readLine()) != null) {
							// For any other K value
							final StringTokenizer itr = new StringTokenizer(line, ",");
							List<String> item_list=new ArrayList<>();
							while (itr.hasMoreTokens()) {
								String item = itr.nextToken(); //Get the item
								item_list.add(item); // Add the item to the list
							}
							item_list.remove(item_list.size()-1); //Remove the support from the list
							String last_item = item_list.get(item_list.size()-1); //Get the last/k-1 item
							item_list.remove(item_list.size()-1); // Remove the k-1 item from the list
							String item_key = item_list.stream().collect(Collectors.joining(",","","")); // Convert the list to a string
							List<String> temp;//Storing new Key in HashMap
							if(prev_itemset.containsKey(item_key)){ //Checking if key already exists in HashMap
								temp = prev_itemset.get(item_key);
							}
							else{
								temp = new ArrayList<>();
							}
							temp.add(last_item);
							prev_itemset.put(item_key,temp); //Add the value to the hashmap


						}
					}
				}
			} catch(IOException e){
				throw new RuntimeException(e);
			}
		}

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			final StringTokenizer itr = new StringTokenizer(value.toString(),",");
			Combination combination = new Combination(); // Call the class combination which returns a set with the different combination of elements of the given array
			Set<List<Integer>> combinations;

			List<Integer> item_list=new ArrayList<>();
			while (itr.hasMoreTokens()) {
				int item = Integer.parseInt(itr.nextToken()); //Get the item
				item_list.add(item); // Add the item to the list
			}
			item_list.remove(item_list.size()-1); //Remove the support from the list
			int last_item = item_list.get(item_list.size()-1); //Get the last/k-1 item

			item_list.remove(item_list.size()-1); // Remove the k-1 item from the list
			String item_key = item_list.stream().map(n-> String.valueOf(n)).collect(Collectors.joining(",","","")); // Convert the list to a string
			item_list.add(last_item); // Add the k-1 element back to the list
			List<String> cache_value = prev_itemset.get(item_key); //Getting the Values from the HashMap
			if (cache_value != null){
				for (int i=0;i<cache_value.size();i++) {
					int value_out = Integer.parseInt(cache_value.get(i)); // Get the value from the list of values
					if (last_item < value_out) {
						item_list.add(value_out); // Generate the superset (the candidate)
						combinations = combination.findCombinations(item_list.toArray(new Integer[0]), k-1); // Get all the combinations of size k-1 of the candiate
						int combination_count = combinations.size(); // Get the number of combinations generated
						int ctr = 0;
						for (List<Integer> c : combinations) {
							int c_last_item = c.get(c.size()-1); //Get the last/k-1 item
							c.remove(c.size()-1);
							String subset = c.stream().map(n -> String.valueOf(n)).collect(Collectors.joining(",", "", "")); // Convert the list to a string
							List<String> prev_value = prev_itemset.get(subset); //Getting the values
							if (prev_value != null) {
								for (int j = 0; j < prev_value.size(); j++) {
									int prev_out = Integer.parseInt(prev_value.get(j)); // Get the value from the list of values
									if (c_last_item == prev_out) { // Check if the candidate exists in Lk-1
										ctr++;
									}
								}
							}
						}
						if (ctr == combination_count) { // If all the subsets of the candidate exist in Lk-1
							String new_candidate = item_list.stream().map(n -> String.valueOf(n)).collect(Collectors.joining(",", "", ""));
							context.write(nw, new Text(new_candidate));
						}
						item_list.remove(item_list.size() - 1); // Remove the added element so the next value can be added to create the new candidate

					}
				}
			}
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Candidate Generation");
		job.setJarByClass(AprioriGen.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		jobConf.set("Previous Itemset", args[0]);
		jobConf.set("K", args[2]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}