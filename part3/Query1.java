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
	    JobConf conf = new JobConf(Query1.class);
	    conf.setJobName("query1");
		conf.setOutputKeyClass(IntWritable.class);
	    conf.setOutputValueClass(Text.class);
	
	    conf.setMapperClass(Map.class);
	    // conf.setCombinerClass(Reduce.class);
	    // conf.setReducerClass(Reduce.class);
	
	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	
	    FileInputFormat.setInputPaths(conf, new Path(args[0]));
	    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	    JobClient.runJob(conf);
	}
}