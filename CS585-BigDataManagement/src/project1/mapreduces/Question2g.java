package project1.mapreduces;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question2g {
	public static class MyMapperQuestion2g extends
			Mapper<Object, Text, IntWritable, IntWritable> {
//		private static final int INTEREST_THRESHOLD = 2000000;
//		private static final IntWritable keyOne = new IntWritable(1);
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			if (tokens.length != 5)
				return;
			if (tokens[1].length() < 10 && tokens[2].length() < 10
					&& tokens[3].length() >= 20 && tokens[4].length() < 10) {
				//access log dataset
				int personId = Integer.parseInt(tokens[1]);
				int accessTime = Integer.parseInt(tokens[4]);
				context.write(new IntWritable(personId), new IntWritable(accessTime)); 

			}
		}
	}

	public static class MyReducerQuestion2g extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		
		private static final int INTEREST_THRESHOLD = 900000;
		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int maxAccessTime = -1;
			for (IntWritable val : values) {
				int accessTime = val.get();
				maxAccessTime = accessTime > maxAccessTime ? accessTime : maxAccessTime;  
			}
			
			if (maxAccessTime <= INTEREST_THRESHOLD) {
				context.write(key, null);
			}
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage:  Question2g [input] [output]");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Question 2g");
		job.setJarByClass(Question2g.class); //change the class here
		job.setMapperClass(MyMapperQuestion2g.class);
//		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(MyReducerQuestion2g.class);
		
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
