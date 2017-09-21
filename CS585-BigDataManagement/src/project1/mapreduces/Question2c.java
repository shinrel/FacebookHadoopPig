package project1.mapreduces;

import java.io.IOException;
import java.util.*;

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
import java.util.TreeMap;

public class Question2c {
	public static class MyMapperQuestion2c extends
			Mapper<Object, Text, IntWritable, IntWritable> {
		private HashMap<Integer, Integer> topInterestingPages = new HashMap<Integer, Integer>();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			if (tokens.length != 5)
				return;
			if (tokens[1].length() < 10 && tokens[2].length() < 10
					&& tokens[3].length() >= 20 && tokens[4].length() < 10) {
				// access log dataset
				int whatpage = Integer.parseInt(tokens[2]);
				if (topInterestingPages.containsKey(whatpage)) {
					int count = topInterestingPages.get(whatpage);
					topInterestingPages.put(whatpage, count + 1);
				} else topInterestingPages.put(whatpage, 1);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Integer whatpage : topInterestingPages.keySet()) {
				context.write(new IntWritable(whatpage), new IntWritable(
						topInterestingPages.get(whatpage)));
			}
		}

	}

	public static class MyReducerQuestion2c extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private HashMap<Integer, Integer> topInterestingPages = new HashMap<Integer, Integer>();

		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			topInterestingPages.put(key.get(), sum);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// select top 10:
			int i = 0;
			while (i++ < 10 || topInterestingPages.size() <= 0 ) {
				int maxVal = -1;
				int maxKey = -1;
				for (Integer whatpage : topInterestingPages.keySet()) {
					int count = topInterestingPages.get(whatpage);
					if (count > maxVal) {
						maxVal = count;
						maxKey = whatpage;
					}
					
				}
				context.write(new IntWritable(maxKey), new IntWritable(maxVal));
				topInterestingPages.remove(maxKey);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: Question2c.jar [input] [output]");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Question 2c");
		job.setJarByClass(Question2c.class); // change the class here
		job.setMapperClass(MyMapperQuestion2c.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(MyReducerQuestion2c.class);
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
