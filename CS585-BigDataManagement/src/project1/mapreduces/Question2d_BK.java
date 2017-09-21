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

public class Question2d_BK {
	public static class MyMapperQuestion2d extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text id = new Text();
		private Text name = new Text();
		// private final static Text one = new Text("1");
		private Hashtable<String, String> lookupTbl = new Hashtable<String, String>();

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			/*
			 * Configuration conf = context.getConfiguration(); //String
			 * mypagePath = conf.get("/user/hadoop/input/mypage_test.csv");
			 * String mypagePath = conf.get(
			 * "/home/hadoop/eclipse-workspace/TestMapReduce/datasets/proj1/mypage_test.csv"
			 * ); try { Path path = new Path(mypagePath); FileSystem fs =
			 * FileSystem.get(conf); BufferedReader br = new BufferedReader(new
			 * InputStreamReader(fs.open(path))); String line = ""; while ((line
			 * = br.readLine()) != null) { String[] tokens = line.split(",");
			 * String ID = tokens[0]; String name = tokens[1]; lookupTbl.put(ID,
			 * name); } } catch (Exception ex) { ex.printStackTrace(); }
			 */
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
				String name = lookupTbl.get(tokens[2]);
				id.set(tokens[2] + "," + name);
				context.write(id, one); // value is one
			}
			// if (tokens[1].length() >= 10 && tokens[2].length() >= 10) {
			// //MyPage datasets.
			// id.set((tokens[0])); //key is ID
			// name.set(tokens[1]);
			// context.write(id, name); //value is username
			// }

		}
	}

	public static class MyCombinerQuestion2d extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			
			for (IntWritable val : values) {
				// String s = val.toString();
				// //check if s is number, add to sum, otherwise, get the name
				// if (s.length() < 10) {
				// sum += Integer.parseInt(s);
				// } else {
				// name = s;
				// }
				sum += val.get();
			}
			result.set(sum);
			//Text newKey = new Text(key.toString().split(":")[1]);
			//String newKeyStr = key.toString() + ":*";
			//Text newKey = new Text(newKeyStr);

			//context.write(newKey, result);
			context.write(key, result);
		}
	}

	public static class MyReducerQuestion2d extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			String name = "";
			for (IntWritable val : values) {
				// String s = val.toString();
				// //check if s is number, add to sum, otherwise, get the name
				// if (s.length() < 10) {
				// sum += Integer.parseInt(s);
				// } else {
				// name = s;
				// }
				sum += val.get();
			}
			result.set(sum);
//			Text newKey = new Text(key.toString().split(":")[1]);
			Text newKey = new Text(key.toString());

			context.write(newKey, result);
			// context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		final String NAME_NODE = "hdfs://localhost:8020";
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: ");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "question 2d");
		job.setJarByClass(Question2d_BK.class); // change the class here
		job.setMapperClass(MyMapperQuestion2d.class);
		job.setCombinerClass(MyCombinerQuestion2d.class);
		job.setReducerClass(MyReducerQuestion2d.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

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
