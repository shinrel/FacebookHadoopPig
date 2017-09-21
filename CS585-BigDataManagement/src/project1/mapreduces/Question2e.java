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

public class Question2e {
	public static class MyMapperQuestion2e extends
			Mapper<Object, Text, IntWritable, Text> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			if (tokens.length != 5)
				return;
			if (tokens[1].length() < 10 && tokens[2].length() < 10
					&& tokens[3].length() >= 20 && tokens[4].length() < 10) {
				//access log dataset
				int bywho = Integer.parseInt(tokens[1]);
				String whatpage = (tokens[2]);
				Text val = new Text("1" + "," + whatpage);
				IntWritable personId = new IntWritable(bywho); 
				context.write(personId, val);
			}
		}
	}

	public static class MyReducerQuestion2e extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		private Text result = new Text();

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			Set<String> distinctPages = new HashSet<>();
			for (Text val : values) {
				String sval = val.toString();
				String tokens[] = sval.split(",");
				sum += Integer.parseInt(tokens[0]);
				distinctPages.add(tokens[1]);
			}
			result.set(sum + "," + distinctPages.size());
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: ");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Question 2e");
		job.setJarByClass(Question2e.class); //change the class here
		job.setMapperClass(MyMapperQuestion2e.class);
//		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(MyReducerQuestion2e.class);
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
