import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Query1 {
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
	    private IntWritable countryID = new IntWritable();
	  	private Text customerInfo = new Text();
   	
	  	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		  	String line = value.toString();
		  	String[] result = line.split(",");
		  	int countryCode = Integer.parseInt(result[3]);
		  	if (countryCode >= 2 && countryCode <= 6) {
			  	countryID.set(countryCode);
			  	customerInfo.set(line);
			  	context.write(countryID, customerInfo);
		  	}
 		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = new Job(conf, "query1");
		job.setJarByClass(Query1.class);
    	job.setMapperClass(Map.class);
	
	    // job.setCombinerClass(Reduce.class); // dont need combiner or reducer for this
	    // job.setReducerClass(Reduce.class);
	
	    job.setOutputKeyClass(IntWritable.class);
    	job.setOutputValueClass(Text.class);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}