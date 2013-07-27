package org.apache.hadoop.examples.tfidf;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.terasort.TeraGen;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ProgramDriver;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A driver class to initiate the appropriate job based on the user's input.
 * 
 * @author hero
 */
public class TfidfDriver extends Configured implements Tool {

	/**
	 * @param args
	 */
	public int run(String[] args) {

		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();

		try {
			// Add all the join main classes
			pgd.addClass("tf-idf-1", WordFrequenceInDoc.class,
					"TF-IDF 1 --- Word Frequence In Documents");
			pgd.addClass("tf-idf-2", WordCountsForDocs.class,
					"TF-IDF 2 --- Word Counts In Documents");
			pgd.addClass("tf-idf-3", WordsInCorpusTFIDF.class,
					"TF-IDF 3 --- Word in Corpus and TF-IDF");

			// Execute the appropriate class
			pgd.driver(args);

			// Success
			exitCode = 0;
		} catch (Throwable e) {
			e.printStackTrace();
			return(exitCode);
		}
		return(exitCode);
	}
	
	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new JobConf(), new TfidfDriver(), args);
	    System.exit(res); 
	}
}
