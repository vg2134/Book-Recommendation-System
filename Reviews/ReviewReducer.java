import java.util.*;
import java.io.IOException;
import java.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.*;



public class ReviewReducer extends Reducer<IntWritable, Text, NullWritable, Text>{
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
	
		
			for(Text value: values) {
				Object obj1 = JSONValue.parse(value.toString());
				JSONObject jobj = (JSONObject)obj1;
				
				String user_id = (String)jobj.get("user_id");
				String book_id = (String)jobj.get("book_id");
				
				String read_at = (String)jobj.get("read_at");
				String[] read_year_split = read_at.split(" ");
				String readYear = read_year_split[read_year_split.length - 1];
				if(readYear == "") {
					readYear = "0";
				}
				
				
				context.write(NullWritable.get(), new Text(String.valueOf(user_id + "," + book_id + "," + readYear)));
			}
		
	}

}
