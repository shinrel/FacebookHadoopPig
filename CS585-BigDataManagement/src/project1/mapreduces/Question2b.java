package project1.mapreduces;

import java.io.IOException;
import java.util.StringTokenizer;
import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Hashtable;

import org.apache.hadoop.filecache.DistributedCache;

public class Question2b {
	public static class MyMapperQuestion2b extends
			Mapper<Object, Text, IntWritable, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			if (tokens.length != 5)
				return;
			if (tokens[1].length() >= 10 && tokens[2].length() >= 10) {
				// MyPage datasets.
				int countryCode = Integer.parseInt(tokens[3]);
				context.write(new IntWritable(countryCode), one);
				
			}

		}
	}

	

	public static class MyReducerQuestion2b extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage:  Question2b [input] [output] ");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "question 2b");
		job.setJarByClass(Question2b.class); // change the class here
		job.setMapperClass(MyMapperQuestion2b.class);
		//job.setCombinerClass(MyReducerQuestion2b.class);
		job.setReducerClass(MyReducerQuestion2b.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);


		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
