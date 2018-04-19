import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Query2b { // operates on transactions: TransID, CustID, TransValue, TransNumItems, TransDesc
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
	    
	
	    public void map(LongWritable key, Text value, OutputCollector<IntWritable, FloatWritable> output, Reporter reporter) throws IOException {
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
	        output.collect(kcustID, vtransValue);
	    }
	}

	public class Combiner extends Reducer<IntWritable, Text, IntWritable, Text>{

		public void reduce(IntWritable key, Iterator<FloatWritable> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException, InterruptedException {
    		String out = "";
    		boolean first = true;
    		while(values.hasNext()) {
    			if(!first) out+=",";
    			else first = false;
    			out+=values.next();
    		}

    		output.collect(key, new Text(out));

		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
	    public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
	    	int count = 0;
	    	float total = 0;
	    	try {
	    		while(values.hasNext()) {
	    			String[] result = values.next().split(",");
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

	    	output.collect(key, fval); // key is the customer id, value is CustID,NumTransactions,TotalValue
	    }
	}
	
	public static void main(String[] args) throws Exception {
	    JobConf conf = new JobConf(Query2b.class);
	    conf.setJobName("query2b");
		conf.setOutputKeyClass(IntWritable.class);
	    conf.setOutputValueClass(Text.class);
	
	    conf.setMapperClass(Map.class);
	    conf.setCombinerClass(Combiner.class);
	    conf.setReducerClass(Reduce.class);
	
	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	
	    FileInputFormat.setInputPaths(conf, new Path(args[0]));
	    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	    JobClient.runJob(conf);
	}
}