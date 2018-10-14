package course.dataintensive.tfidf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TFIDF extends Configured {
		
	public TFIDF() {}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		Path inputPath = new Path(args[0]);
		String outputDir = args[1];

		Configuration conf = new Configuration();
		conf.set("corpus", args[0]);
				
		Path wordCountDoc = new Path(outputDir, "wordCount");	// generates the file "wordCount" inside the provided output directory
		Path resultTFIDF = new Path(outputDir, "tf-idf");		// generates the result file "tfidf" inside the provided output directory
		
		Job wordCountJob = Job.getInstance(conf, "Word Count Job");

		wordCountJob.setJarByClass(TFIDF.class);

		wordCountJob.setMapperClass(WordCountMapper.class);
		wordCountJob.setReducerClass(WordCountReducer.class);

		wordCountJob.setMapOutputKeyClass(Text.class);
		wordCountJob.setMapOutputValueClass(IntWritable.class);
		wordCountJob.setOutputKeyClass(Text.class);
		wordCountJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(wordCountJob, inputPath);
		wordCountJob.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(wordCountJob, wordCountDoc);
		wordCountJob.setOutputFormatClass(TextOutputFormat.class);
		
		wordCountJob.waitForCompletion(true);
		
		
		Job tfidfJob = Job.getInstance(conf, "TFIDF Job");
		tfidfJob.setJarByClass(TFIDF.class);

		tfidfJob.setMapperClass(TFIDFMapper.class);
		tfidfJob.setReducerClass(TFIDFReducer.class);

		tfidfJob.setOutputKeyClass(Text.class);
		tfidfJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(tfidfJob, wordCountDoc);
		tfidfJob.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(tfidfJob, resultTFIDF);
		tfidfJob.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(tfidfJob.waitForCompletion(true) ? 0 : 1);
			
	}

}
