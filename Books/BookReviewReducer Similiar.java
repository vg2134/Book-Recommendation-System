import java.io.IOException;
import java.util.Iterator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.*;

public class BookReviewReducer
    extends Reducer<IntWritable, Text, NullWritable, Text> {

  @Override
  public void reduce(IntWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
          
    try{
    
        for (Text value : values) {
          Object obj = JSONValue.parse(value.toString());
          JSONObject data = (JSONObject)obj;
          String book_id = (String)data.get("book_id");
          JSONArray similar_books = (JSONArray) data.get("similar_books");
          Iterator<String> iterator = similar_books.iterator();
          while(iterator.hasNext()) {
                context.write(NullWritable.get(), new Text(book_id+","+iterator.next()));
                //System.out.println(book_id+","+iterator.next());
            }
        }
      
      }
      catch(Exception e){
                e.printStackTrace();
        }

  }
}