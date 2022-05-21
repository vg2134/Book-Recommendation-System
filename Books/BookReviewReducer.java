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

        context.write(NullWritable.get(), new Text("country_code"+","+"language_code"+","+"is_ebook"+","+"average_rating"+","+"format"+","+"book_id"+","+"ratings_count"+","+"work_id"+","+"title"));
    
        for (Text value : values) {
          Object obj = JSONValue.parse(value.toString());
           
          JSONObject data = (JSONObject)obj;
          String country_code = (String)data.get("country_code");
          String language_code = (String)data.get("language_code");
          String is_ebook = (String)data.get("is_ebook");
          String average_rating = (String)data.get("average_rating");
          String format = (String)data.get("format");
        //   JSONArray similar_books = (JSONArray) data.get("similar_books");
        //   JSONArray authors = (JSONArray) data.get("authors");
        //   Object obj2 = JSONValue.parse(authors.get(0).toString());
        //   JSONObject data2 = (JSONObject)obj2;
        //   String author_id = (String)data2.get("author_id");
          String book_id = (String)data.get("book_id");
          String ratings_count = (String)data.get("ratings_count");
          String work_id = (String)data.get("work_id");
          String line = (String)data.get("title");
          String title = line.replace(",", "");
          context.write(NullWritable.get(), new Text(country_code+","+language_code+","+is_ebook+","+average_rating+","+format+","+book_id+","+ratings_count+","+work_id+","+title));
          
  
        }
      
      }
      catch(Exception e){
                e.printStackTrace();
        }

  }
}