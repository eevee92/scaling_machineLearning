package mapred.itemBase;

import java.io.IOException;
import mapred.job.Optimizedjob;
import mapred.util.FileUtil;
import mapred.util.SimpleParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.mahout.cf.taste.hadoop.item.ToVectorAndPrefReducer;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VectorWritable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		String tmpdir = parser.get("tmpdir");

		getUserScoreVector(input, tmpdir + "/user_vector");

		getCooccurrenceMatrix(tmpdir + "/user_vector",tmpdir + "/cooccurrence");

  	getWarpCooccurrence(tmpdir + "/cooccurrence",tmpdir + "/warp_occ");
		getUserSplit(tmpdir + "/user_vector",tmpdir + "/split_user");

		String[] inputs = {tmpdir + "/warp_occ", tmpdir + "/split_user"};
		getVectorAndPref(inputs, tmpdir + "/vector_pref");

		getMulAndRecommand(tmpdir + "/vector_pref",output);

	}

	/**
	 * Same as getJobFeatureVector, but this one actually computes feature
	 * vector for all hashtags.
	 * 
	 * @param input
	 * @param output
	 * @throws Exception
	 */
	private static void getUserScoreVector(String input, String output)
			throws Exception {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Get user score movie vector");
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job,new Path( input ));

		job.setOutputFormatClass(SequenceFileOutputFormat.class);	
		SequenceFileOutputFormat.setOutputPath(job, new Path(output));

		job.setOutputKeyClass(VarLongWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setClasses(WikipediaToItemPrefsMapper.class, WikipediaToUserVectorReducer.class, null);
		job.setMapOutputClasses(VarLongWritable.class, VarLongWritable.class);
		job.run();
	}

	/**
	 * Same as getJobFeatureVector, but this one actually computes feature
	 * vector for all hashtags.
	 * 
	 * @param input
	 * @param output
	 * @throws Exception
	 */
	private static void getCooccurrenceMatrix(String input, String output)
			throws Exception {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Get co-occurrence matrix");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job,new Path( input ));		
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setClasses(UserVectorToCooccurrenceMapper.class, UserVectorToCooccurrenceReducer.class, null);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setMapOutputClasses(IntWritable.class, IntWritable.class);
		//System.out.println(conf.get("mapred.map.tasks"));
		job.run();
	}

	private static void getWarpCooccurrence(String input, String output)
			throws Exception {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Get co-occurrence warp vector");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job,new Path( input ));

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(output));
/* //for debug:
				job.setOutputFormatClass(TextOutputFormat.class);
				TextOutputFormat.setOutputPath(job, new Path(output));
*/
		job.setClasses(CooccurrenceColumnWrapperMapper.class, null, null);
		job.setMapOutputClasses(IntWritable.class, VectorOrPrefWritable.class);
		job.setMapOutputClasses(IntWritable.class, VectorOrPrefWritable.class);
		job.setMapOutputClasses(IntWritable.class, VectorOrPrefWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorOrPrefWritable.class);
		//System.out.println(conf.get("mapred.map.tasks"));
		job.run();
	}

	private static void getUserSplit(String input, String output)
			throws Exception {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Get user split vector");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job,new Path( input ));

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(output));
/*  // for debug:
				job.setOutputFormatClass(TextOutputFormat.class);
				TextOutputFormat.setOutputPath(job, new Path(output));
*/
		job.setClasses(UserVectorSplitterMapper.class, null, null);
		job.setMapOutputClasses(IntWritable.class, VectorOrPrefWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorOrPrefWritable.class);
		//System.out.println(conf.get("mapred.map.tasks"));
		job.run();
	}

	private static void getVectorAndPref(String[] inputs, String output)
			throws Exception {
		Optimizedjob job = new Optimizedjob(new Configuration(), inputs, output,
				"Get vector and preference");
		job.setInputFormatClass(SequenceFileInputFormat.class);

		for (String input : inputs)
			SequenceFileInputFormat.addInputPath(job,new Path( input ));		
/*  // for debug:
				job.setOutputFormatClass(TextOutputFormat.class);
				TextOutputFormat.setOutputPath(job, new Path(output));		
*/
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(output));
	
		job.setClasses(ToVectorAndPrefMapper.class, ToVectorAndPrefReducer.class, null);
		job.setMapOutputClasses(VarIntWritable.class,VectorOrPrefWritable.class);
		job.setOutputKeyClass(VarIntWritable.class);
		job.setOutputValueClass(VectorAndPrefsWritable.class);
		//System.out.println(conf.get("mapred.map.tasks"));
		job.run();
	}

	private static void getMulAndRecommand(String input, String output)
			throws Exception {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Do matrix multiply and recommand");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job,new Path( input ));
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(output));
		
		job.setClasses(PartialMultiplyMapper.class, AggregateAndRecommendReducer.class, AggregateCombiner.class);
		job.setMapOutputClasses(VarLongWritable.class, VectorWritable.class);
		//System.out.println(conf.get("mapred.map.tasks"));
		job.run();
  }
}
