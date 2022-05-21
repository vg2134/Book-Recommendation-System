import java.io.IOException;
import java.util.Iterator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.*;
enum Author {
    COUNT
}

public class BookReviewReducer
    extends Reducer<IntWritable, Text, NullWritable, Text> {

  @Override
  public void reduce(IntWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    
        for (Text value : values) {
          Object obj = JSONValue.parse(value.toString());
          JSONObject data = (JSONObject)obj;
          String book_id = (String)data.get("book_id");
          JSONArray authors = (JSONArray) data.get("authors");
          
          Iterator<JSONObject> iterator = authors.iterator();
          
          while(iterator.hasNext()) {
                JSONObject obj2 = iterator.next();
                String author_id = (String) obj2.get("author_id");
                context.write(NullWritable.get(), new Text(book_id+","+author_id));
            }
          if(authors.size() != 0) {
              context.getCounter(Author.COUNT).increment(1);
          }
        }
  }
}