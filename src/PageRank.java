//Shashank Gupta 800970543
package org.myorg;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.myorg.WikiCount;
import org.myorg.GraphLink;
import org.myorg.PageRankInitializer;
import org.myorg.PageRankAlgo;
import org.myorg.Sorter;

public class PageRank {

	//Driver for all programs
	public static void main(String[] args) throws Exception {
		
		int i;
		String input;
		String output;
		String graphLink;
		String wikiCount;
		String pageRankInitializer;
		String pageRankAlgo;
		String iterIn;
		String iterOut;
		String sIn;
		int status;
		
		PageRank pageRank = new PageRank();
		
		input  = args[0];
		output = args[1];
		
		//Setting output paths
		graphLink = "/user/cloudera/assign3/run4/GraphLink";
		wikiCount = "/user/cloudera/assign3/run4/WikiCount";
		pageRankInitializer = "/user/cloudera/assign3/run4/PageRankInitializer";
		pageRankAlgo = "/user/cloudera/assign3/run4/PageRankAlgo";
		
		//Run GraphLink
		status = ToolRunner.run(new GraphLink(), new String[] {input,graphLink});

		//Run WikiCount
		status = ToolRunner.run(new WikiCount(), new String[] {input,wikiCount});

		//Run PageRankInitializer
		status = ToolRunner.run(new PageRankInitializer(), new String[] {graphLink,wikiCount,pageRankInitializer});

		//PageRank calculation for 10 iterations
		for (i=1; i<=10; i++) {
			if (i == 1) {
				//For the first iteration using pageRankInitializer
				iterIn = pageRankInitializer;
			} else {
				iterIn = pageRankAlgo + "/Iter_" + Integer.toString(i-1);
			}
			iterOut = pageRankAlgo + "/Iter_" + Integer.toString(i);
			
			status = ToolRunner.run(new PageRankAlgo(), new String[] {iterIn,iterOut});
			
			//Cleaning the intermediate iteration outputs
			pageRank.clean(new Path(iterIn));
		}
		
		sIn = pageRankAlgo + "/Iter_" + Integer.toString(i-1);
		status = ToolRunner.run(new Sorter(), new String[] {sIn,output});
	}

	//Cleaning the intermediate iterations
	public void clean(Path path) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(path);
	}
	
}

