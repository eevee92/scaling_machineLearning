package mapred.itemBase;

import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.VarIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ToVectorAndPrefMapper extends Mapper<IntWritable,VectorOrPrefWritable,VarIntWritable,VectorOrPrefWritable> {
  
  public void map(IntWritable key, VectorOrPrefWritable value, Context context) throws IOException, InterruptedException {
		int num=key.get();
      context.write(new VarIntWritable(num), value);
   
  }
}
