package assignment2.assignment2;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import assignment2.assignment2.MiscUtils;



public class Top10Locations{


		public static class Top10Mapper extends Mapper<LongWritable, Text,  Text, IntWritable> {
			

			private final static IntWritable one = new IntWritable(1);
			private Text zipCode = new Text();	
			
			protected void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
				
				String recordInput = value.toString();
				String[] fields = recordInput.split("\\^");
			   
				if (fields.length > 0) {
					//String zip = fields[1].replaceAll("[^0-9]", "");
					String zip = fields[1].substring(fields[1].length() - 5);	
						zipCode.set(zip);
						context.write(zipCode, one );
							
						}
					
				}
			} 	
		
		public static class Top10Reducer extends Reducer< Text, IntWritable,  Text, IntWritable> {
			private HashMap<String, Integer> countMap = new HashMap<String, Integer>();
			
			public void reduce(Text key, Iterable<IntWritable> values,	Context context) throws IOException, InterruptedException {
				
				 int sum = 0;
			      for (IntWritable val : values) {
			        sum += val.get();
			      }
			      
			      countMap.put(key.toString(), sum);
				
			}
			
			
			
			  protected void cleanup(Context context) throws IOException, InterruptedException {

				
				 //TreeMap<String, Integer> sortedMap = new TreeMap<String, Integer>();
				 //sortedMap.putAll(countMap);
				  Map<String, Integer> sortedMap = MiscUtils.sortByValues(countMap);

				  
				 int i = 0;
				 for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
						context.write(new Text(entry.getKey().trim()), new IntWritable(entry.getValue()));
						i++;
						if (i == 10)
							break;
					}
			      
		      }
			
			
		}

		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

			Configuration conf = new Configuration();
		    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
		    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
		    conf.set("mapreduce.framework.name", "yarn");
		    Job job = Job.getInstance(conf, "yelp 3");
		    job.setJarByClass(Top10Locations.class);
		    job.setMapperClass(Top10Mapper.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    job.setReducerClass(Top10Reducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    job.setNumReduceTasks(1);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

	}


	

