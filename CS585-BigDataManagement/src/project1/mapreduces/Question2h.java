package project1.mapreduces;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question2h {
	/************* JOB 1: calculate the average number of friends across all owners ***********/
	public static class MyMapper1Question2h extends
			Mapper<Object, Text, IntWritable, IntWritable> {

		// private final static IntWritable one = new IntWritable(1);
		private final static IntWritable oneKey = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			if (tokens.length != 5)
				return;
			if (tokens[1].length() < 10 && tokens[2].length() < 10
					&& tokens[3].length() < 10) {
				// Friend datasets.
				int personId = Integer.parseInt(tokens[1]);
				int myFriendId = Integer.parseInt(tokens[2]);
				context.write(oneKey, new IntWritable(personId));
			}
		}
	}

	public static class MyReducer1Question2h extends
			Reducer<IntWritable, IntWritable, IntWritable, DoubleWritable> {
		
		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			Set<Integer> distinctUsers = new HashSet<Integer>();
			for (IntWritable val : values) {
				// count distinct users in here:
				int ID = (int) val.get();
				if (!distinctUsers.contains(ID)) {
					count++;
					distinctUsers.add(ID);
				}
			}

			context.write(null, new DoubleWritable((double) 20000000
					/ (double) count));
		}
	}

	/*******************************************************************************/
	/****************** Job2: find famous and happy people **************************/
	public static class MyMapper2Question2h extends
			Mapper<Object, Text, IntWritable, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			if (tokens.length != 5)
				return;
			if (tokens[1].length() < 10 && tokens[2].length() < 10
					&& tokens[3].length() < 10) {
				// Friend datasets.
				int personId = Integer.parseInt(tokens[1]);
				int myFriendId = Integer.parseInt(tokens[2]);
				context.write(new IntWritable(personId), one);
			}
		}
	}

	public static class MyReducer2Question2h extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private double averageNumFriends;
		@Override
		protected void setup(Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException { 
			Configuration configuration = context.getConfiguration();    
			FSDataInputStream inputStream = FileSystem.get(configuration).open(
					new Path("/user/hadoop/output/Question2h/job1/part-r-00000"));
			
			BufferedReader bufferReader = new BufferedReader(new InputStreamReader(inputStream));
			averageNumFriends = Double.parseDouble(bufferReader.readLine());
			bufferReader.close();
			
		}
		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			if (sum > averageNumFriends) {
				context.write(key, null);
			}
		}
	}

	/*******************************************************************************/
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		for (String s : otherArgs) {
			System.out.println(s);
		}
		if (otherArgs.length < 3) {
			System.err.println("Usage: [input] [output_job1] [output_job2]");
			System.exit(1);
		}
		Job job1 =  new Job(conf, "Question 2h - calculating average");
		
		job1.setJarByClass(Question2h.class); // change the class here
		job1.setMapperClass(MyMapper1Question2h.class);
		// job1.setCombinerClass(IntSumReducer.class);
		job1.setReducerClass(MyReducer1Question2h.class);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
		
		Job job2 = new Job(conf, "Question 2h - find famous and happy people");
		job2.setJarByClass(Question2h.class); // change the class here
		job2.setMapperClass(MyMapper2Question2h.class);
		// job2.setCombinerClass(IntSumReducer.class);
		job2.setReducerClass(MyReducer2Question2h.class);
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job2, new Path(otherArgs[0]));	
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
		
		boolean res = job1.waitForCompletion(true);
//		job2.waitForCompletion(true);
		if (res) { res = job2.waitForCompletion(true);}
		System.exit(res ? 0 : 1);
	}
}
