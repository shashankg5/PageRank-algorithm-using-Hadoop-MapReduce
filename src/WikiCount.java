//Shashank Gupta 800970543
package org.myorg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.util.ToolRunner;


//Counting the number of wikipages by counting the number of lines
public class WikiCount extends Configured implements Tool {
	
	public static void main( String[] args) throws  Exception {
	      int res  = ToolRunner.run( new WikiCount(), args);
	      System.exit(res);
	   }

	public static class WikiCountMap extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		@Override
		public void map(LongWritable offset, Text line, Context context)
							throws IOException, InterruptedException {
			if (line.toString().length() > 0) {
				// Emit '<1>, <1>' for each line
				context.write(new IntWritable(1), new IntWritable(1));
			}
		}
	}

	public static class WikiCountReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable> {

		@Override 
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
								throws IOException, InterruptedException {
		
			int count = 0;
			//Normal iteration and incrementing the count
			for (IntWritable i : values) {
				count++;
			}
			// Emit '<TOTAL>, <total number of wikipages>'
			context.write(new Text("TOTAL"), new IntWritable(count));
		
		}
		
	}


	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "$XXX$");
		conf.set("mapreduce.output.textoutputformat.separator", "$XXX$");
		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("WikiCount");
		job.setMapperClass(WikiCountMap.class);
		job.setReducerClass(WikiCountReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}

}

