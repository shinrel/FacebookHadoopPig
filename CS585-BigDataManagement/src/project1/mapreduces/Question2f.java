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

public class Question2f {
	public static class MyMapperQuestion2f extends
			Mapper<Object, Text, IntWritable, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			if (tokens.length != 5)
				return;
			if (tokens[1].length() < 10 && tokens[2].length() < 10
					&& tokens[3].length() < 10) {
				// Friend datasets.
				int personId = Integer.parseInt(tokens[1]);
				int friendId = Integer.parseInt(tokens[2]);
				Text val = new Text("f:" + friendId);
				context.write(new IntWritable(personId), val);
			}
			if (tokens[1].length() < 10 && tokens[2].length() < 10
					&& tokens[3].length() >= 20 && tokens[4].length() < 10) {
				//access log dataset
				int personId = Integer.parseInt(tokens[1]);
				int friendIdPage = Integer.parseInt(tokens[2]);
				Text val = new Text("a:" + friendIdPage);
				context.write(new IntWritable(personId), val);
			}
		}
	}

	public static class MyReducerQuestion2f extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			Set<Integer> accessFriendIds = new HashSet<Integer>();
			Set<Integer> listFriendIds = new HashSet<Integer>();
			for (Text val : values) {
				String s = val.toString();
				if (s.startsWith("a:")) {
					//access log.
					accessFriendIds.add(Integer.parseInt(s.split(":")[1]));
				} else if (s.startsWith("f:")) {
					//friends 
					listFriendIds.add(Integer.parseInt(s.split(":")[1]));
				}
			}
			for (int friendId : listFriendIds) {
				if (!accessFriendIds.contains(friendId)) {
					Text ret = new Text("" + friendId);
					context.write(key, ret);
				}
			}
			
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage:  Question2f [input] [output]");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Question 2f");
		job.setJarByClass(Question2f.class); //change the class here
		job.setMapperClass(MyMapperQuestion2f.class);
//		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(MyReducerQuestion2f.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
