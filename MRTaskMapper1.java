package com.orienit.MRTask_1.Thirupathi;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MRTaskMapper1 extends Mapper<LongWritable, Text, Text, LongWritable> {

	private final static LongWritable one = new LongWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = line.split("\\|");
		String country = words[2].toString();
		String status = words[3].toString();
		String pair;
		pair = country + " " + status;
		context.write(new Text(pair), one);
	}
}
