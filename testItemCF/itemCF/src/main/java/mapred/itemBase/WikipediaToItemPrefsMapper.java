package mapred.itemBase;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.*;
import java.io.IOException;


public class WikipediaToItemPrefsMapper extends Mapper<LongWritable,Text,VarLongWritable,VarLongWritable> {
  private static final Pattern NUMBERS = Pattern.compile("(\\d+)");
  
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    
    Matcher m = NUMBERS.matcher(line);

    m.find();
    VarLongWritable userID = new VarLongWritable(Long.parseLong(m.group())); 
    m.find();
    long temp1 = Long.parseLong(m.group());
    m.find();
    long temp2 = Long.parseLong(m.group());
    long temp3 = temp1*10+temp2;

    VarLongWritable itemIDandPre = new VarLongWritable(temp3);

    context.write(userID, itemIDandPre);

  }
}
