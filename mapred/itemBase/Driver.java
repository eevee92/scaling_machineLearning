package mapred.itemBase;

import java.io.IOException;
import java.lang.Boolean;

import mapred.job.Optimizedjob;
import mapred.util.FileUtil;
import mapred.util.SimpleParser;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
// import org.apache.mahout.cf.taste.hadoop.item.ToVectorAndPrefReducer;

//import org.apache.mahout.math.VarLongWritable;
import org.apache.hadoop.io.VLongWritable;

import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;

//import org.apache.mahout.math.VarIntWritable;
import org.apache.hadoop.io.VIntWritable;

import org.apache.mahout.math.VectorWritable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Driver {

	public static void main(String args[]) throws Exception {

		long start = System.currentTimeMillis();		

		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		String tmpdir = parser.get("tmpdir");

		getUserScoreVector(input, tmpdir + "/user_vector");    // job1


		// Job2-1 Job2-2 working in pipe
		getCooccurrenceMatrix(tmpdir + "/user_vector",tmpdir + "/cooccurrence");  // Job 2-1
		getUserSplit(tmpdir + "/user_vector",tmpdir + "/split_user");  // job 2-2
		// Job 3
		// change (warpped) co-occuerence Vector and split_user Vector (both are VectorOrPrefWritable) into VectorAndPrefWritable
		String[] inputs = {tmpdir + "/cooccurrence", tmpdir + "/split_user"};
		getVectorAndPref(inputs, tmpdir + "/vector_pref");


/*
		// Job 2-1 and Job 2-2 working simultaneously
		boolean flag = getCooccurrenceMatrixAndUserSplit(tmpdir + "/user_vector",tmpdir + "/cooccurrence", tmpdir + "/split_user");
		if (flag )
		{
				String[] inputs = {tmpdir + "/cooccurrence", tmpdir + "/split_user"};
				getVectorAndPref(inputs, tmpdir + "/vector_pref");
		}
*/

		
		getMulAndRecommend(tmpdir + "/vector_pref",output);
		
		long end = System.currentTimeMillis();
		System.out.println(String.format("Runtime for All Jobs : %d ms",  end - start));

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
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output, "Get user score movie vector");
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job,new Path( input ));
/*
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(output));
*/
		job.setOutputFormatClass(SequenceFileOutputFormat.class);	
		SequenceFileOutputFormat.setOutputPath(job, new Path(output));

		job.setOutputKeyClass(VLongWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setClasses(WikipediaToItemPrefsMapper.class, WikipediaToUserVectorReducer.class, null);
		job.setMapOutputClasses(VLongWritable.class, VLongWritable.class);


		Configuration getUserScoreVectorConf = job.getConfiguration();
		// compress Mapper's output
		getUserScoreVectorConf.setBoolean("mapreduce.map.output.compress",true);
		getUserScoreVectorConf.set("mapreduce.output.fileoutputformat.compress.type","BLOCK");

		getUserScoreVectorConf.setInt("mapreduce.task.io.sort.mb",200);
		getUserScoreVectorConf.setDouble("mapreduce.map.sort.spill.percent",0.99);

		job.run();
	}



	/**
	 * 
	 *  getCooccurrenceMatrix
	 * 
	 * @param input
	 * @param output
	 * @throws Exception
	 */
	private static void getCooccurrenceMatrix(String input, String output)
			throws Exception {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,	"Get co-occurrence matrix");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job,new Path( input ));		
/*		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(output));
*/
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(output));

	
		job.setClasses(UserVectorToCooccurrenceMapper.class, UserVectorToCooccurrenceReducer.class, null);
		job.setOutputKeyClass(VIntWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setMapOutputClasses(VIntWritable.class, VIntWritable.class);

		Configuration getCooccurrenceMatrixConf = job.getConfiguration();
		// compress Mapper's output
		getCooccurrenceMatrixConf.setBoolean("mapreduce.map.output.compress",true);
		getCooccurrenceMatrixConf.set("mapreduce.output.fileoutputformat.compress.type","BLOCK");

		getCooccurrenceMatrixConf.setInt("mapreduce.task.io.sort.mb",200);
		getCooccurrenceMatrixConf.setDouble("mapreduce.map.sort.spill.percent",0.99);

		//System.out.println(conf.get("mapred.map.tasks"));
		job.run();
	}


	private static void getUserSplit(String input, String output)
			throws Exception {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,"Get user split vector");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job,new Path( input ));

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(output));
/*  // for debug:
				job.setOutputFormatClass(TextOutputFormat.class);
				TextOutputFormat.setOutputPath(job, new Path(output));
*/
		job.setClasses(UserVectorSplitterMapper.class, null, null);
		job.setMapOutputClasses(VIntWritable.class, VectorOrPrefWritable.class);

		Configuration getUserSplitConf = job.getConfiguration();
		getUserSplitConf.setInt("mapreduce.task.io.sort.mb",200);
		getUserSplitConf.setDouble("mapreduce.map.sort.spill.percent",0.99);
		job.setOutputKeyClass(VIntWritable.class);
		job.setOutputValueClass(VectorOrPrefWritable.class);
		//System.out.println(conf.get("mapred.map.tasks"));
		job.run();
	}


  // simultaneously running two jobs
	private static boolean getCooccurrenceMatrixAndUserSplit(String input, String output1, String output2)
			throws Exception{

		// job 2
		Optimizedjob job2 = new Optimizedjob(new Configuration(), input, output2,"Get user split vector");
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job2,new Path( input ));
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job2, new Path(output2));
		job2.setClasses(UserVectorSplitterMapper.class, null, null);
		job2.setMapOutputClasses(VIntWritable.class, VectorOrPrefWritable.class);
		Configuration getUserSplitConf = job2.getConfiguration();
		getUserSplitConf.setInt("mapreduce.task.io.sort.mb",200);
		getUserSplitConf.setDouble("mapreduce.map.sort.spill.percent",0.99);
		job2.setOutputKeyClass(VIntWritable.class);
		job2.setOutputValueClass(VectorOrPrefWritable.class);	
		job2.submit();


		// job 1
		Optimizedjob job1 = new Optimizedjob(new Configuration(), input, output1,	"Get co-occurrence matrix");
		job1.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job1,new Path( input ));		
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job1, new Path(output1));
		job1.setClasses(UserVectorToCooccurrenceMapper.class, UserVectorToCooccurrenceReducer.class, null);
		job1.setOutputKeyClass(VIntWritable.class);
		job1.setOutputValueClass(VectorWritable.class);
		job1.setMapOutputClasses(VIntWritable.class, VIntWritable.class);
		Configuration getCooccurrenceMatrixConf = job1.getConfiguration();
		getCooccurrenceMatrixConf.setBoolean("mapreduce.map.output.compress",true);
		getCooccurrenceMatrixConf.set("mapreduce.output.fileoutputformat.compress.type","BLOCK");
		getCooccurrenceMatrixConf.setInt("mapreduce.task.io.sort.mb",200);
		getCooccurrenceMatrixConf.setDouble("mapreduce.map.sort.spill.percent",0.99);
		job1.submit();



		boolean flag1 = job1.isComplete();
		boolean flag2 = job2.isComplete();
		while( !flag1 || !flag2 )
		{
				flag1 = job1.isComplete();
				flag2 = job2.isComplete();
		}
		return ( flag1 && flag2 );
	}










	private static void getVectorAndPref(String[] inputs, String output)
			throws Exception {
		Optimizedjob job = new Optimizedjob(new Configuration(), inputs, output, "Get vector and preference");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		Configuration getVectorAndPrefConf = job.getConfiguration();

		// set input path, and Mappers for each path
		MultipleInputs.addInputPath(job, new Path(inputs[0]),SequenceFileInputFormat.class, CooccurrenceColumnWrapperMapper.class);
		MultipleInputs.addInputPath(job, new Path(inputs[1]),SequenceFileInputFormat.class, ToVectorAndPrefMapper.class); 

		// set Mapper's Output key, value		
		job.setMapOutputClasses(VIntWritable.class,VectorOrPrefWritable.class);

		job.setReducerClass(ToVectorAndPrefReducer.class);
		// set Reducer's output key, value		
		job.setOutputKeyClass(VIntWritable.class);
		job.setOutputValueClass(VectorAndPrefsWritable.class);
/*  // for debug:
				job.setOutputFormatClass(TextOutputFormat.class);
				TextOutputFormat.setOutputPath(job, new Path(output));		
*/
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(output));
		
		// compress Mapper's output
		getVectorAndPrefConf.setBoolean("mapreduce.map.output.compress",true);
		getVectorAndPrefConf.set("mapreduce.output.fileoutputformat.compress.type","BLOCK");
		// Opt Mapper
		getVectorAndPrefConf.setInt("mapreduce.task.io.sort.mb",200);
		getVectorAndPrefConf.setDouble("mapreduce.map.sort.spill.percent",0.99);
		
		//System.out.println(conf.get("mapred.map.tasks"));
		job.run();
	}




	private static void getMulAndRecommend(String input, String output)
			throws Exception {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,"Do matrix multiply and recommend");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job,new Path( input ));
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(output));
		
		job.setClasses(PartialMultiplyMapper.class, AggregateAndRecommendReducer.class, AggregateCombiner.class);
		job.setMapOutputClasses(VLongWritable.class, VectorWritable.class);
		Configuration getMulAndRecommendConf = job.getConfiguration();
		// compress Mapper's output
		getMulAndRecommendConf.setBoolean("mapreduce.map.output.compress",true);
		getMulAndRecommendConf.set("mapreduce.output.fileoutputformat.compress.type","BLOCK");
		getMulAndRecommendConf.setInt("mapreduce.task.io.sort.mb",200);
		getMulAndRecommendConf.setDouble("mapreduce.map.sort.spill.percent",0.99);

		//System.out.println(conf.get("mapred.map.tasks"));
		job.run();
  }
}
