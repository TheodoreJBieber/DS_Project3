import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Query1 {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	    private Text word = new Text();
	    private Text custInfo = new Text();
	
	    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
	        String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line, ",");
	        custInfo.set(tokenizer.nextToken());
	        custInfo.append("," + tokenizer.nextToken());
	        custInfo.append("," + tokenizer.nextToken());
	        String countryCode = tokenizer.nextToken();
	        custInfo.append("," + tokenizer.nextToken());
	        IntWritable k = new IntWritable();
	        try {
	        	k.set(Integer.parseInt(countryCode));
	        } catch (NumberFormatException e) {
	        	//e.printStackTrace();
	        }
	        output.collect(k, custInfo);
	    }
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	    public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
	    	int code = key.get();
	    	if(code >= 2 && code <= 6) {
	    		for(value : values) {
	    			output.collect(key, value);
	    		}
	    	}
	    }
	}
	
	public static void main(String[] args) throws Exception {
	    JobConf conf = new JobConf(Query1.class);
	    conf.setJobName("query1");
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