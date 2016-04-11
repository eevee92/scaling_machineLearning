package mapred.itemBase;

import org.apache.hadoop.io.LongWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.regex.*;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import java.io.IOException;

public class WikipediaToUserVectorReducer extends Reducer<VarLongWritable,VarLongWritable,VarLongWritable,VectorWritable> { 
  public void reduce(VarLongWritable userID,
                     Iterable<VarLongWritable> itemPrefs,
                     Context context)throws IOException, InterruptedException {
    
    Vector userVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
    for (VarLongWritable itemPref : itemPrefs) {
      userVector.set(((int)itemPref.get())/10, ((int)itemPref.get())%10);
    }
    context.write(userID, new VectorWritable(userVector)); 
  }
}
