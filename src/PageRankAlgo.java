//Shashank Gupta 800970543
package org.myorg;

import java.io.IOException;
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


public class PageRankAlgo extends Configured implements Tool {
	
		public static void main( String[] args) throws  Exception {
	      int res  = ToolRunner.run( new PageRankAlgo(), args);
	      System.exit(res);
	   }

	public static class PageRankAlgoMap extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable offset, Text line, Context context)
							throws IOException, InterruptedException {
			String title;
			String wikiPageDetails;
			String[] outlinks = null;
			String[] rankOut;
			double pageRank;
			double weightage;
			String[] sArr;

			String wikiLine = line.toString();
			// Extracting title and contents
			sArr = wikiLine.split("\\$XXX\\$");
			title = sArr[0];
			wikiPageDetails = sArr[1];
			
			//Extracting current page rank and outgoing links
			//rankOut is a combination of rank and outlinks
			rankOut = wikiPageDetails.split("\\$\\#\\#\\#\\$", 2);
			
			pageRank = Double.parseDouble(rankOut[0]);
			
			//if outlinks exists
			if (rankOut.length > 1 && !rankOut[1].isEmpty()) {
				outlinks = rankOut[1].split("\\$\\#\\#\\#\\$");
			} 
			
			//Emit <Title>, <!>  for actual wiki pages
			context.write(new Text(title), new Text("!"));
			
			//Dividing page rank equally among outlinks
			if ((rankOut.length > 1 && !rankOut[1].isEmpty())) {
				weightage = pageRank / outlinks.length;
			
				for (String outlink : outlinks) {
					//Emit <Outlink>, <Weightage>
					context.write(new Text(outlink), new Text(Double.toString(weightage)));
				}
			}
			
			//Emit <Title>, <List of outgoing links>
			//Using "##$##" as a separator
			if (rankOut.length > 1 && !rankOut[1].isEmpty()) {
				context.write(new Text(title), new Text("##$##" + rankOut[1]));
			} else {
				context.write(new Text(title), new Text("##$##"));
			}
		}
		
	}

	public static class PageRankAlgoReducer extends Reducer<Text, Text, Text, Text> {

		private double dampingFactor = 0.85; //Given

		@Override 
		public void reduce(Text title, Iterable<Text> values, Context context)
								throws IOException, InterruptedException {
			double contributions = 0.0;
			double pageRank;
			boolean isWikiPage = false;
			String outlinks = "";
			String v;

			//Iterate list of values
			for (Text val : values) {
				v = val.toString().trim();
				
				//If found, then it is a wikipage and changing the value of the boolean isWikiPage
				if(v.equals("!")) {
					isWikiPage = true;
					continue;
				}
				
				try {
					//Will throw an exception if v is not a number
					contributions += Double.parseDouble(v);
				} catch(NumberFormatException e) {
					//Getting the outlinks
					outlinks = v.split("\\#\\#\\$\\#\\#", 2)[1];
				}
			}
			
			//If not wikipage, we dont need to calculate the pagerank
			if(!isWikiPage){
				return;
			}
			
			//Compute new page rank
			pageRank = (1.0 - dampingFactor) + (dampingFactor * contributions);
			
			//Emit ‘<Title>, <New Page Rank, List of outgoing link>’
			context.write(new Text(title), new Text(Double.toString(pageRank) + "$###$" + outlinks));
		}
		
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		//Using the separator instead of the default tab would help in handling the cases where the title consist of tabs
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "$XXX$");
		conf.set("mapreduce.output.textoutputformat.separator", "$XXX$");

		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("PageRankAlgo");
		job.setMapperClass(PageRankAlgoMap.class);
		job.setReducerClass(PageRankAlgoReducer.class);
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

