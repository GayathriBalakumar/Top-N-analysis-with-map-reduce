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


public class Top10RatedBusinesses{


		public static class Top10Mapper extends Mapper<LongWritable, Text,  Text, DoubleWritable> {
			

			private Text businessID = new Text();
			private final static DoubleWritable stars = new DoubleWritable();
				
			
			protected void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
				
				String recordInput = value.toString();
				String[] fields = recordInput.split("\\^");
			   
				if (fields.length > 0) {
					
					businessID.set(fields[2]);
					stars.set(Double.parseDouble(fields[3]));
					
						context.write(businessID, stars );
							
						}
					
				}
			} 	
		
		public static class Top10Reducer extends Reducer< Text, DoubleWritable,  Text, DoubleWritable > {
			private HashMap<String, Double> avgMap = new HashMap<String, Double>();
			
			public void reduce(Text key, Iterable<DoubleWritable> values,	Context context) throws IOException, InterruptedException {
				
				int count =0; 
				int sum = 0;
				double avg = 0.0;
			      for (DoubleWritable val : values) {
			    	count++;  
			        sum += val.get();
			      }
			      
			     avg = sum/count; 
			      avgMap.put(key.toString(), avg);
				
			}
			
			
			
			  protected void cleanup(Context context) throws IOException, InterruptedException {

				
				 //TreeMap<String, Integer> sortedMap = new TreeMap<String, Integer>();
				 //sortedMap.putAll(countMap);
				  Map<String, Double> sortedMap = MiscUtils.sortByValues(avgMap);

				  
				 int i = 0;
				 for (Map.Entry<String, Double> entry : sortedMap.entrySet()) {
						context.write(new Text(entry.getKey().trim()), new DoubleWritable(entry.getValue()));
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
		    job.setJarByClass(Top10RatedBusinesses.class);
		    job.setMapperClass(Top10Mapper.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(DoubleWritable.class);
		    job.setReducerClass(Top10Reducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(DoubleWritable.class);
		    job.setNumReduceTasks(1);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

	}


	

