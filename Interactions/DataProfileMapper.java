import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//count number of unique authors and books
public class DataProfileMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //user_id,book_id,is_read,rating,is_reviewed
        String[] vals = value.toString().split(",");

        context.write(new Text("users"), new Text(vals[0]));
        context.write(new Text("books"), new Text(vals[1]));
    }

}
