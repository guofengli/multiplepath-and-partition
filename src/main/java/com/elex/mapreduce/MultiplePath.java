package com.elex.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MultiplePath {
	
	public static class MultipleMap extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer strTMP = new StringTokenizer(value.toString(), "\t");
			String outputKeyStr = strTMP.nextToken();
//			strTMP.nextToken();
			StringBuffer strBuffer = new StringBuffer();
			while(strTMP.hasMoreTokens()){
				strBuffer.append(strTMP.nextToken() + "\t");
			}
//			String outputValueStr = strTMP.nextToken();
			Text outputKey = new Text();
			Text outputValue = new Text();
			outputKey.set(outputKeyStr);
			outputValue.set(strBuffer.toString());
			context.write(outputKey, outputValue);
		}
	}
	
	public static class MultipleMap1 extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer strTMP = new StringTokenizer(value.toString(), "\t");
			String outputKeyStr = strTMP.nextToken();
//			strTMP.nextToken();
			StringBuffer strBuffer = new StringBuffer();
			while(strTMP.hasMoreTokens()){
				strBuffer.append(strTMP.nextToken());
			}
//			String outputValueStr = strTMP.nextToken();
			Text outputKey = new Text();
			Text outputValue = new Text();
			outputKey.set(outputKeyStr);
			outputValue.set(strBuffer.toString());
			context.write(outputKey, outputValue);
		}
	}
	
	public static class MultipleReduce extends Reducer<Text, Text, Text, Text>{
		private MultipleOutputs<Text,Text> mos;
		public void setup(Context context){
			mos = new MultipleOutputs<Text,Text>(context);
		}
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for(Text value:values){
				String [] tmps = value.toString().split("\t");
				if(tmps.length > 2) {
					mos.write("file1", key, value);
				} else{
					mos.write("file2", key, value);
				}
//				context.write(key, value);
			}
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Multiple input path");
		job.setJarByClass(MultiplePath.class);
		job.setReducerClass(MultipleReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path("/user/hive/warehouse/ares_merge/000000_0"), TextInputFormat.class, MultipleMap.class);
//		MultipleInputs.addInputPath(job, new Path("/user/hive/warehouse/ares_merge/000000_0"), TextInputFormat.class, MultipleMap.class);
		MultipleInputs.addInputPath(job, new Path("/tmp/output/part-r-00000"), TextInputFormat.class, MultipleMap1.class);
		MultipleOutputs.addNamedOutput(job, "file1", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "file2", TextOutputFormat.class, Text.class, Text.class);
		FileOutputFormat.setOutputPath(job, new Path("/tmp/multipleinputpath"));
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
