package com.orienit.MRTask_1.Thirupathi;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MRTaskJob1 implements Tool {
	// Initializing configuration object
	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf; // getting the configuration
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf; // setting the configuration
	}

	@Override
	public int run(String[] args) throws Exception {

		// initializing the job configuration
		Job mrTaskJob1 = new Job(getConf());

		// setting the job name
		mrTaskJob1.setJobName("Orien IT WordCount Job");

		// to call this as a jar
		mrTaskJob1.setJarByClass(this.getClass());

		// setting custom mapper class
		mrTaskJob1.setMapperClass(MRTaskMapper1.class);

		// setting custom reducer class
		mrTaskJob1.setReducerClass(MRTaskReducer1.class);

		// setting custom combiner class
		// wordCountJob.setCombinerClass(WordCountReducer.class);

		// setting no of reducers
		// wordCountJob.setNumReduceTasks(2);

		// setting custom partitioner class
		// wordCountJob.setPartitionerClass(WordCountPartitioner.class);

		// setting mapper output key class: K2
		mrTaskJob1.setMapOutputKeyClass(Text.class);

		// setting mapper output value class: V2
		mrTaskJob1.setMapOutputValueClass(LongWritable.class);

		// setting reducer output key class: K3
		mrTaskJob1.setOutputKeyClass(Text.class);

		// setting reducer output value class: V3
		mrTaskJob1.setOutputValueClass(LongWritable.class);

		// setting the input format class ,i.e for K1, V1
		mrTaskJob1.setInputFormatClass(TextInputFormat.class);

		// setting the output format class
		// wordCountJob.setOutputFormatClass(TextOutputFormat.class);

		MultipleOutputs.addNamedOutput(mrTaskJob1, "SUCCESS", TextOutputFormat.class, Text.class, LongWritable.class);
		MultipleOutputs.addNamedOutput(mrTaskJob1, "ERROR", TextOutputFormat.class, Text.class, LongWritable.class);

		// setting the input file path
		FileInputFormat.addInputPath(mrTaskJob1, new Path(args[0]));

		// setting the output folder path
		FileOutputFormat.setOutputPath(mrTaskJob1, new Path(args[1]));

		Path outputpath = new Path(args[1]);
		// delete the output folder if exists
		outputpath.getFileSystem(conf).delete(outputpath, true);

		// to execute the job and return the status
		return mrTaskJob1.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		// start the job providing arguments and configurations
		int status = ToolRunner.run(new Configuration(), new MRTaskJob1(), args);
		System.out.println("My Status: " + status);
	}

}