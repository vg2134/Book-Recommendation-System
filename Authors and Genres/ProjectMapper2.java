import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProjectMapper2
    extends Mapper<LongWritable, Text, IntWritable, Text> {
      
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString();

    //write each line to the reducer
    context.write(new IntWritable(1), new Text(line));
    
  }
}