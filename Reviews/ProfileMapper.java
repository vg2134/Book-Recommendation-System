
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ProfileMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	private Text output_userId = new Text();
	
	@Override
    public void map(LongWritable key, Text value, Context context) 
	throws IOException, InterruptedException {
    	Map<String, String> parsed = transformXmlToMap(value.toString())
    			
    	String user_id = parsed.get("user_id");
    	output_userId.set(user_id);

		context.write(output_userId, NullWritable.get()); 	
    }
};

    