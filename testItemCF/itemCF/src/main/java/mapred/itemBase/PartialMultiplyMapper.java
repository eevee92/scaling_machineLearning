package mapred.itemBase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.math.Vector;
import java.util.List;
import java.io.IOException;

public class PartialMultiplyMapper extends
    Mapper<VarIntWritable,VectorAndPrefsWritable,
           VarLongWritable,VectorWritable> {
            
  @Override
  public void map(VarIntWritable key, VectorAndPrefsWritable vectorAndPrefsWritable, Context context) throws IOException, InterruptedException {
		
    Vector cooccurrenceColumn = vectorAndPrefsWritable.getVector(); 
		List<Long> userIDs = vectorAndPrefsWritable.getUserIDs(); 
		List<Float> prefValues = vectorAndPrefsWritable.getValues();
    	
    for (int i = 0; i < userIDs.size(); i++) {
      long userID = userIDs.get(i);
      float prefValue = prefValues.get(i);
      Vector partialProduct = cooccurrenceColumn.times(prefValue); 
      context.write(new VarLongWritable(userID), new VectorWritable(partialProduct));
  	}
	}
}
