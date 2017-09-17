package com.orienit.MRTask_1.Thirupathi;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MRTaskReducer1 extends
		Reducer<Text, LongWritable, Text, LongWritable> {

	private MultipleOutputs<Text, LongWritable> mos;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		mos = new MultipleOutputs<Text, LongWritable>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {

		// 1.sum the list of values
		long sum = 0;
		long sum1 = 0;
		if (key.toString().contains("SUCCESS")) {
			for (LongWritable value : values) {
				sum = sum + value.get();
			}
			mos.write("SUCCESS", key, new LongWritable(sum));
		} else {
			for (LongWritable value : values) {
				sum1 = sum1 + value.get();
			}
			mos.write("ERROR", key, new LongWritable(sum1));
		}
		

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
}
