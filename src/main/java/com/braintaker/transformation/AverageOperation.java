package com.braintaker.transformation;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageOperation {
	public static class MapperOperation extends
			Mapper<LongWritable, Text, IntWritable, LongWritable> {

		@Override
		protected void setup(Context context) {
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
						String line = value.toString();
			String linevalue[] = line.split(",");			
			context.write(new IntWritable(Integer.parseInt(linevalue[0])),
					new LongWritable(Long.parseLong(linevalue[2])));
		}

		@Override
		public void cleanup(Context context) {
		}

	}

	public static class ReducerOperation extends
			Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
		@Override
		protected void setup(Context context) {
		}

		@Override
		protected void reduce(IntWritable key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			long sum = 0;
			Integer n=0;
			Iterator<LongWritable> iterator = values.iterator();
			while (iterator.hasNext()) {
				sum += iterator.next().get();
				n++;
			}
			Long avg=sum/n;
			context.write(key, new LongWritable(avg));
		}

		@Override
		public void cleanup(Context context) {
		}
	}
}
