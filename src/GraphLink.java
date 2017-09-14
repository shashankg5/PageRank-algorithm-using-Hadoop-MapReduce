//Shashank Gupta 800970543
package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.lang.StringUtils;

public class GraphLink extends Configured implements Tool {


	public static void main( String[] args) throws  Exception {
	      int res  = ToolRunner.run( new GraphLink(), args);
	      System.exit(res);
	   }
	   
	   
	//Would emit ‘<Title>, <Outgoing link>’
	public static class GraphLinkMap extends Mapper<LongWritable, Text, Text, Text> {	
		//Links to other Wikipedia articles are of the form "[[Name of other article]]"
		private static final Pattern LINKPAT = Pattern.compile("\\[\\[.*?]\\]");
		
		@Override
		public void map(LongWritable offset, Text line, Context context)
							throws IOException, InterruptedException {
			String title;
			String text;
			String outlink;
			String wikiLine = line.toString();
			if(wikiLine.isEmpty() || wikiLine==null){
				return;			
			}

			title = StringUtils.substringBetween(line.toString(), "<title>", "</title>").trim();
			if (title.isEmpty()) {
				return;
			} 

			
			Matcher lMatcher = LINKPAT.matcher(wikiLine);
			while (lMatcher.find()) {
				// Dropping the brackets and any nested ones if any
				outlink = lMatcher.group().replace("[[", "").replace("]]", "");
				outlink = outlink.trim();
				if (outlink.isEmpty()) {
					continue;
				}
				// Emit '<Title>, <Outgoing link>'
				context.write(new Text(title), new Text(outlink));
			}
			
			// Emit '<Title>, <"">'.
			// Handling nodes which does not have any outlinks
			context.write(new Text(title), new Text(""));
		}
		
	}

	public static class GraphLinkReducer extends Reducer<Text, Text, Text, Text> {

		@Override 
		public void reduce(Text title, Iterable<Text> links, Context context)
								throws IOException, InterruptedException {
			List<String> outlinks = new ArrayList<String>();
			// Remove empty tokens that may be present
			for (Text link : links) {
				if (link.toString().isEmpty()) {
					continue;
				}
				outlinks.add(link.toString());
			}
			//Emit '<Title>, <List of outgoing links>' 
			context.write(title, new Text(StringUtils.join(outlinks, "$###$")));
		}
		
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//Using the separator instead of the default tab would help in handling the cases where the title consist of tabs
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "$XXX$");
		conf.set("mapreduce.output.textoutputformat.separator", "$XXX$");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("GraphLink");
		job.setMapperClass(GraphLinkMap.class);
		job.setReducerClass(GraphLinkReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}

}

