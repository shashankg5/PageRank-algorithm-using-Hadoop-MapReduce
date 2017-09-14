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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.util.ToolRunner;

public class PageRankInitializer extends Configured implements Tool {
	
	public static void main( String[] args) throws  Exception {
	      int res  = ToolRunner.run( new PageRankInitializer(), args);
	      System.exit(res);
	   }

	public static class PageRankInitializerMap extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable offset, Text line, Context context)
							throws IOException, InterruptedException {
			String title;
			String outlinks = "";
			String[] wikiDetails;

			String wikiLine = line.toString();
			
			wikiDetails = wikiLine.split("\\$XXX\\$");
			
			// Extract title and outgoing links in wikipedia page
			title = wikiDetails[0];
			// if links exists
			if (wikiDetails.length == 2) {
				outlinks = wikiDetails[1];
			}
			
			// Emit '<Title>, <List of outgoing links>'
			context.write(new Text(title), new Text(outlinks.trim()));
		}
		
	}

	public static class PageRankInitializerReducer extends Reducer<Text, Text, Text, Text> {

		private int wikiSize;
		//Setting up the wikisize
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			wikiSize    = conf.getInt("WikiSize", 1);
		}

		@Override 
		public void reduce(Text title, Iterable<Text> links, Context context)
								throws IOException, InterruptedException {
			double initialPageRank;
			String outlinks;

			// Initial page rank, 1.0/Total pages
			initialPageRank = 1.0 / wikiSize;
			
			// Emit ‘<Title>, <Initial Page Rank, List of outgoing links>’
			for (Text link : links) {
				outlinks = link.toString().trim();
				context.write(new Text(title), new Text(Double.toString(initialPageRank) + "$###$" + outlinks));
			}
		}
		
	}

	public int run(String[] args) throws Exception {
		int wikiCount = getWikiCount(args[1]);
		
		Configuration conf = new Configuration();
		conf.setInt("WikiSize", wikiCount);
		
		//Using the separator instead of the default tab would help in handling the cases where the title consist of tabs
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "$XXX$");
		conf.set("mapreduce.output.textoutputformat.separator", "$XXX$");

		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("PageRankInitializer");
		job.setMapperClass(PageRankInitializerMap.class);
		job.setReducerClass(PageRankInitializerReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	//Reading the file to get the count from WikiCount
	private int getWikiCount(String wikiCountPath) throws IOException {
		FileSystem fs = null;
		String[] wikiArr = new String[2];
		BufferedReader br = null;
		String line = null;
		Path p;
		
		try {
			p = new Path(wikiCountPath);
			
            fs = p.getFileSystem(new Configuration());
            
            FileStatus[] fileStat = fs.listStatus(p);
            Path[] allFiles = FileUtil.stat2Paths(fileStat);
            
            for (Path file : allFiles) {
            	if (fs.isFile(file)) {
            		br = new BufferedReader(new InputStreamReader(fs.open(file)));
            		line = br.readLine();
            		
            		if (line != null && !line.isEmpty()) {
            			if (line.contains("$XXX$")) {
							//"TOTAL"
            				wikiArr[0] = line.split("\\$XXX\\$")[0];
							//Actual value
            				wikiArr[1] = line.split("\\$XXX\\$")[1];
            				break;
            			}
            		}
            	}
            }
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		//Return the actual count
		return Integer.parseInt(wikiArr[1]);
	}
}

