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

public class Question2d {
	public static class MyMapperQuestion2d extends
			Mapper<Object, Text, IntWritable, Text> {

		
		// private final static Text one = new Text("1");
		private Hashtable<String, String> lookupTbl = new Hashtable<String, String>();

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			
			Path[] uris = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());

			try {
				BufferedReader readBuffer = new BufferedReader(new FileReader(
						uris[0].toString()));
				String line;
				while ((line = readBuffer.readLine()) != null) {
					String[] tokens = line.split(",");
					String ID = tokens[0];
					String name = tokens[1];
					lookupTbl.put(ID, name);
				}
				readBuffer.close();
			} catch (Exception e) {
				System.out.println(e.toString());
			}
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			if (tokens.length != 5)
				return;
			if (tokens[1].length() < 10 && tokens[2].length() < 10
					&& tokens[3].length() < 10) {
				// Friend datasets.
				// id.set((tokens[2])); //key is ID:name
				// get the name:
				int myfriend = Integer.parseInt(tokens[2]);
				String name = lookupTbl.get(tokens[2]);
				Text val = new Text(name + ",1");
				context.write(new IntWritable(myfriend), val); // value is one
			}
			

		}
	}

	public static class MyCombinerQuestion2d extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			String name = "";
			for (Text val : values) {
				String[] tokens = val.toString().split(",");
				name = tokens[0];
				sum += Integer.parseInt(tokens[1]);
			}
			
			context.write(key, new Text(name + "," + sum));
		}
	}

	public static class MyReducerQuestion2d extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			String name = "";
			for (Text val : values) {
				String[] tokens = val.toString().split(",");
				name = tokens[0];
				sum += Integer.parseInt(tokens[1]);
			}
			
			context.write(key, new Text(name + "," + sum));
		}
	}

	public static void main(String[] args) throws Exception {
		final String NAME_NODE = "hdfs://localhost:8020";
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage:  Question2d [input] [output] ");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "question 2d");
		job.setJarByClass(Question2d.class); // change the class here
		job.setMapperClass(MyMapperQuestion2d.class);
		job.setCombinerClass(MyCombinerQuestion2d.class);
		job.setReducerClass(MyReducerQuestion2d.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		

		DistributedCache
				.addCacheFile(new URI(NAME_NODE
						+ "/user/hadoop/input/all/mypage.csv"),
						job.getConfiguration());

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
