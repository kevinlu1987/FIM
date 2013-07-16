package org.mc2.kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Kmeans {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {

		private final static Point point = new Point();

		private Text word = new Text();

		private List<Point> cluster = new ArrayList<Point>();

		private int round;

		protected void setup(Context context) throws IOException,
				InterruptedException {

			Configuration conf = context.getConfiguration();

			this.round = conf.getInt("kmeans.round", 0);

			String clusterUri = null;
			if (this.round == 0) {
				clusterUri = "hdfs://mcmaster:9000/home/cluster.txt";
			}
			else {
				clusterUri = conf.get("kmeans.otuput.path")	+ this.round + "/part-r-00000";
			}
			System.out.println("Read from : " + clusterUri);
			

			FileSystem fs = FileSystem.get(URI.create(clusterUri), conf);
			// Path path = new Path("/home/hadoop/cluster.txt");
			BufferedReader br = null;

			try {
				br = new BufferedReader(new InputStreamReader(fs.open(new Path(
						clusterUri))));
			} catch (IOException e) {
				e.printStackTrace();
			}

			String line = "";
			try {
				// Read a line
				while ((line = br.readLine()) != null) {

					System.out.println("setup read cluster : " + line);
					StringTokenizer sTokenizer = new StringTokenizer(line);
					sTokenizer.nextToken();

					// process the second position in the line
					String[] num = sTokenizer.nextToken().split(",");
					try {
						double num0 = Double.parseDouble(num[0].trim());
						double num1 = Double.parseDouble(num[1].trim());
						Point point = new Point(num0, num1);
						this.cluster.add(point);
					} catch (ArrayIndexOutOfBoundsException e) {
						break;
					}

				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();

			}
			Iterator it = this.cluster.iterator();
			while (it.hasNext()) {
				Point pt = (Point) it.next();
				System.out.println("(" + pt.getX() + "," + pt.getY() + ")");
			}
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {

				String sline = itr.nextToken();
				System.out.println("map read a string : " + sline);

				String pStr[] = sline.split(":");
				for (int i = 0; i < pStr.length; i++) {
					try {

						System.out.println("process point: " + pStr[i]);
						point.setPosition(pStr[i], ",");

					} catch (FormatIllegalException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						return;
					} catch (NumberFormatException e) {
						e.printStackTrace();
						return;
					}

					Point clusterPoint = null;
					Point minPoint = new Point();
					double minDistance = Double.MAX_VALUE;

					Iterator<Point> it = this.cluster.iterator();
					while (it.hasNext()) {
						clusterPoint = it.next();
						double distance = clusterPoint.distance(point);
						if (distance < minDistance) {
							minDistance = distance;
							minPoint = clusterPoint;
						}
					}

					context.write(new Text(minPoint.toString()),
							new Text(point.toString()));
					System.out.println("write Key: " + minPoint.toString()
							+ ", value: " + point.toString());
				}

			}
		}

	}

	public static class PointCenter extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text();
		private static Accumulator ac = new Accumulator();
		private static Point point = new Point();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text val : values) {

				try {
					point.setPosition(val.toString());
					System.out.println("key = " + key.toString() + ", value = "
							+ val.toString());
				} catch (FormatIllegalException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					continue;
				}
				ac.add(point);
			}

			context.write(key, new Text(ac.getCenter().toString()));
			// System.out.println("========================" +
			// ac.getCenter().toString());
			ac.clear();
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: org.mc2.kmeans.Kmeans <round> <input> <output> ");
			System.exit(2);
		}

		int round = Integer.parseInt(otherArgs[0]);
		System.out.println("Num : " + round);
		
		String inputFodler = otherArgs[1];
		String outputFolder = otherArgs[2];
		
		if (!outputFolder.endsWith("/")) {
			outputFolder = outputFolder + "/";
		}
		
		for (int i = 0; i < round; i++) {

			conf.setInt("kmeans.round", i);
			System.out.println("Round : " + i);
			
			conf.setStrings("kmeans.otuput.path", outputFolder);

			Job job = new Job(conf, "Kmeans cluster" + i);
			job.setJarByClass(Kmeans.class);
			job.setMapperClass(TokenizerMapper.class);
			// job.setCombinerClass(PointCenter.class);
			job.setReducerClass(PointCenter.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(inputFodler));
			FileOutputFormat.setOutputPath(job, new Path(outputFolder + (i + 1)));
			job.waitForCompletion(true);
		}
	}
}
