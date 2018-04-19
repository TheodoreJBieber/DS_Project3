import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Query2a { // operates on transactions: TransID, CustID, TransValue, TransNumItems, TransDesc
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
	    
	
	    public void map(LongWritable key, Text value, OutputCollector<IntWritable, FloatWritable> output, Reporter reporter) throws IOException {
	        String line = value.toString();
	        String[] result = line.split(",");
	        String custID = result[1];
	        String transValue = result[2];
	        IntWritable kcustID = new IntWritable();
	        FloatWritable vtransValue = new FloatWritable();
	        try {
	        	kcustID.set(Integer.parseInt(custID));
	        	vtransValue.set(Float.parseFloat(transValue));
	        } catch (NumberFormatException e) {
	        	//e.printStackTrace();
	        }
	        output.collect(kcustID, vtransValue);
	    }
	}

	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, FloatWritable, IntWritable, Text> {
	    public void reduce(IntWritable key, Iterator<FloatWritable> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
	    	int count = 0;
	    	float total = 0;
	    	while(values.hasNext()) {
	    		count+=1;
	    		total+=values.next();
	    	}
	    	String val = key + "," + count + "," + total;
	    	Text fval = new Text(val);

	    	output.collect(key, fval); // key is the customer id, value is CustID,NumTransactions,TotalValue
	    }
	}
	
	public static void main(String[] args) throws Exception {
	    JobConf conf = new JobConf(Query2a.class);
	    conf.setJobName("query2a");
		conf.setOutputKeyClass(IntWritable.class);
	    conf.setOutputValueClass(Text.class);
	
	    conf.setMapperClass(Map.class);
	    conf.setCombinerClass(Reduce.class);
	    conf.setReducerClass(Reduce.class);
	
	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	
	    FileInputFormat.setInputPaths(conf, new Path(args[0]));
	    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	    JobClient.runJob(conf);
	}
}