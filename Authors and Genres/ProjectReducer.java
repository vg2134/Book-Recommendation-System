import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.*;

public class ProjectReducer
    extends Reducer<IntWritable, Text, NullWritable, Text> {
  
  enum Authors {
    TOTAL_ROWS,
    RATING_BETWEEN_0_AND_1,
    RATING_BETWEEN_1_AND_2,
    RATING_BETWEEN_2_AND_3,
    RATING_BETWEEN_3_AND_4,
    RATING_BETWEEN_4_AND_5,
    EMPTY_NAME_OR_ID
  }

  @Override
  public void reduce(IntWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
          
    context.write(NullWritable.get(), new Text("average_rating"+","+"author_id"+","+"text_reviews_count"+","+"name"+","+"ratings_count"));

    for (Text value : values) {
      Object obj = JSONValue.parse(value.toString());
      JSONObject data = (JSONObject)obj;
      String average_rating = (String)data.get("average_rating");
      if(average_rating==null || average_rating.trim().equals("")){average_rating="0";}
      String author_id = (String)data.get("author_id");
      String text_reviews_count = (String)data.get("text_reviews_count");
      if(text_reviews_count==null || text_reviews_count.trim().equals("")){text_reviews_count="0";}
      String name = (String)data.get("name");
      if(author_id==null||author_id.trim().equals("")||name==null||name.trim().equals("")){
        context.getCounter(Authors.EMPTY_NAME_OR_ID).increment(1);
        continue;
      }
      name = name.replace(","," ");
      
      String ratings_count = (String)data.get("ratings_count");

      float avg_rt_float = Float.parseFloat(average_rating);
      if(0<=avg_rt_float && avg_rt_float<1){
        context.getCounter(Authors.RATING_BETWEEN_0_AND_1).increment(1);
      }else if(1<=avg_rt_float && avg_rt_float<2){
        context.getCounter(Authors.RATING_BETWEEN_1_AND_2).increment(1);
      }else if(2<=avg_rt_float && avg_rt_float<3){
        context.getCounter(Authors.RATING_BETWEEN_2_AND_3).increment(1);
      }else if(3<=avg_rt_float && avg_rt_float<4){
        context.getCounter(Authors.RATING_BETWEEN_3_AND_4).increment(1);
      }else if(4<=avg_rt_float && avg_rt_float<5){
        context.getCounter(Authors.RATING_BETWEEN_4_AND_5).increment(1);
      }

      context.getCounter(Authors.TOTAL_ROWS).increment(1);
      context.write(NullWritable.get(), new Text(average_rating+","+author_id+","+text_reviews_count+","+name+","+ratings_count));
    }
  }
}