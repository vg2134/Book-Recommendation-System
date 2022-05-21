import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class DataProfileReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> set = new HashSet<>();

        for(Text val : values) {
            set.add(val.toString());
        }
        
        context.write(key, new IntWritable(set.size()));
    }
    
}
