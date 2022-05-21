import java.util.*;
import java.io.IOException;
import java.io.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.*;



public class ProfileReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
	@Override
	public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
	{
		context.write(key, NullWritable.get());			
	}
}
