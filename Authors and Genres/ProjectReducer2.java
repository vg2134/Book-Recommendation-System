import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.*;

public class ProjectReducer2
    extends Reducer<IntWritable, Text, NullWritable, Text> {
  
  enum Genres {
    TOTAL_ROWS,
    EMPTY_GENRE,
    EMPTY_ID,
    GENRE_ROMANCE,
    GENRE_POETRY,
    GENRE_MYSTERY,
    GENRE_THRILLER,
    GENRE_CRIME,
    GENRE_HISTORY,
    GENRE_FANTASY,
    GENRE_FICTION
  }

  @Override
  public void reduce(IntWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
          
    context.write(NullWritable.get(), new Text("book_id"+","+"genres"));

    for (Text value : values) {
      Object obj = JSONValue.parse(value.toString());
      JSONObject data = (JSONObject)obj;
      String book_id = (String)data.get("book_id");
      JSONObject genres = (JSONObject)data.get("genres");

      Iterator keys = genres.keySet().iterator();
      String genre=""; 
      if(keys.hasNext()){
        genre = (String)keys.next();
      }

      genre = genre.replace(", ","-");
      if(book_id==null || book_id.trim().equals("")){
        context.getCounter(Genres.EMPTY_ID).increment(1);
        continue;
      }
      
      if(genre.equals("")){
        //Maintain and increment a counter whenever we see a row with an empty genre
        context.getCounter(Genres.EMPTY_GENRE).increment(1);
      }

      //Maintain and increment counters for each genre. For example, genres such as mystery, crime, thriller.. Etc
      String genre_lower=genre.toLowerCase();
      if(genre_lower.contains("romance")){
        context.getCounter(Genres.GENRE_ROMANCE).increment(1);
      }
      if(genre_lower.contains("poetry")){
        context.getCounter(Genres.GENRE_POETRY).increment(1);
      }
      if(genre_lower.contains("mystery")){
        context.getCounter(Genres.GENRE_MYSTERY).increment(1);
      }
      if(genre_lower.contains("thriller")){
        context.getCounter(Genres.GENRE_THRILLER).increment(1);
      }
      if(genre_lower.contains("crime")){
        context.getCounter(Genres.GENRE_CRIME).increment(1);
      }
      if(genre_lower.contains("history")){
        context.getCounter(Genres.GENRE_HISTORY).increment(1);
      }
      if(genre_lower.contains("fantasy")){
        context.getCounter(Genres.GENRE_FANTASY).increment(1);
      }
      if(genre_lower.contains("fiction")){
        context.getCounter(Genres.GENRE_FICTION).increment(1);
      }

      //Maintain and increment a counter for each row written to the file.
      context.getCounter(Genres.TOTAL_ROWS).increment(1);
      context.write(NullWritable.get(), new Text(book_id+","+genre));
    }
  }
}