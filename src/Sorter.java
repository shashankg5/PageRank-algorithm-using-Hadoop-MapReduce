//Shashank Gupta 800970543
package org.myorg;

import java.io.IOException;
import java.util.Comparator;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.ToolRunner;

public class Sorter extends Configured implements Tool {
	
	public static void main( String[] args) throws  Exception {
	      int res  = ToolRunner.run( new Sorter(), args);
	      System.exit(res);
	   }

	public static class SorterMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {

		@Override
		public void map(LongWritable offset, Text line, Context context)
							throws IOException, InterruptedException {
			String title;
			String wikiPageDetails;
			String[] rankOut;
			String[] sArr;
			String pageRank;
			String outlinks;

			String wikiLine = line.toString();
			
			//Extracting title and contents
			sArr = wikiLine.split("\\$XXX\\$");
			title = sArr[0];
			wikiPageDetails = sArr[1];
			
			//Extracting page rank and outlinks
			rankOut = wikiPageDetails.split("\\$\\#\\#\\#\\$", 2);
			pageRank = rankOut[0];
			outlinks = rankOut[1];
			
			//Emit ‘<-Page Rank>,<Title>’
			context.write(new DoubleWritable(-1 * Double.parseDouble(pageRank)), new Text(title));
			//Making page rank negative makes the highest page rank value lowest
			//And thus lists it at the top
		}
		
	}

	
	public static class SorterReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

		@Override 
		public void reduce(DoubleWritable ranks, Iterable<Text> titles, Context context)
								throws IOException, InterruptedException {
			
			double temp = 0.0;
			String t = "";
			//Multiplying by -1 again to bring it to the correct value
			temp = ranks.get() * -1;
			//Iterating on titles
			for (Text title : titles) {
				t = title.toString();
				context.write(new Text(t), new DoubleWritable(temp));
			}
			
		}
	}

	   

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "$XXX$");
		//Also default
		conf.set("mapreduce.output.textoutputformat.separator", "\t");
		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("Sorter");
		job.setMapperClass(SorterMap.class);
		job.setReducerClass(SorterReducer.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//Having one reducer task
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}

}

