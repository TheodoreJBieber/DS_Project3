import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Query2a { // operates on transactions: TransID, CustID, TransValue, TransNumItems, TransDesc
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
	    
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        String[] result = line.split(",");
	        String custID = result[1];
	        String transValue = result[2];
	        IntWritable kcustID = new IntWritable();
	        Text vtransValue = new Text(transValue);
	        try {
	        	kcustID.set(Integer.parseInt(custID));
	        } catch (NumberFormatException e) {
	        	//e.printStackTrace();
	        }
	        context.write(kcustID, vtransValue);
	    }
	}

	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
	    public void reduce(IntWritable key, Iterator<Text> values, Context context) throws IOException, InterruptedException {
	    	int count = 0;
	    	float total = 0;
	    	while(values.hasNext()) {
	    		count += 1;
	    		total += Float.parseFloat(values.next().toString());
	    	}
	    	String val = key + "," + count + "," + total;
	    	Text fval = new Text(val);

	    	context.write(key, fval); // key is the customer id, value is CustID,NumTransactions,TotalValue
	    }
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = new Job(conf, "query2a");
		job.setJarByClass(Query2a.class);
    	job.setMapperClass(Map.class);
    	// job.setCombinerClass(Combiner.class); // no combiner for this part
    	job.setReducerClass(Reduce.class);
    	job.setOutputKeyClass(IntWritable.class);
    	job.setOutputValueClass(Text.class);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}