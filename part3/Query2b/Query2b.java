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

public class Query2b { // operates on transactions: TransID, CustID, TransValue, TransNumItems, TransDesc
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

	public static class Combiner extends Reducer<IntWritable, Text, IntWritable, Text>{

		public void reduce(IntWritable key, Iterator<Text> values, Context context) throws IOException, InterruptedException {
    		String out = "";
    		boolean first = true;
    		while(values.hasNext()) {
    			if(!first) out+=",";
    			else first = false;
    			out+=values.next().toString();
    		}

    		context.write(key, new Text(out));

		}
	}

	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
	    public void reduce(IntWritable key, Iterator<Text> values, Context context) throws IOException, InterruptedException {
	    	int count = 0;
	    	float total = 0;
	    	try {
	    		while(values.hasNext()) {
	    			String[] result = values.next().toString().split(",");
	    			for(int i = 0; i < result.length; i++) {
	    				count+=1;
	    				total+=Float.parseFloat(result[i]);
	    			}
	    			
	    		}
	    	} catch(NumberFormatException e) {
	    		// e.printStackTrace();
	    	}
	    	String val = key + "," + count + "," + total;
	    	Text fval = new Text(val);

	    	context.write(key, fval); // key is the customer id, value is CustID,NumTransactions,TotalValue
	    }
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = new Job(conf, "query2b");
		job.setJarByClass(Query2b.class);
    	job.setMapperClass(Map.class);
    	job.setCombinerClass(Combiner.class);
    	job.setReducerClass(Reduce.class);
    	job.setOutputKeyClass(IntWritable.class);
    	job.setOutputValueClass(Text.class);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}