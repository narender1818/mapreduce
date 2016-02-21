package com.braintaker.driver;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import static com.braintaker.transformation.AverageOperation.MapperOperation;
import static com.braintaker.transformation.AverageOperation.ReducerOperation;

public class JoinerDriver {
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration config = new Configuration();		
		Job job = Job.getInstance(config, "Join Operation");
		job.setJarByClass(JoinerDriver.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(MapperOperation.class);
		job.setReducerClass(ReducerOperation.class);
	
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(LongWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
