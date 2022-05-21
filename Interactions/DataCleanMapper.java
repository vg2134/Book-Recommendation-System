import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


enum Records {
    IS_READ_FALSE,
    MALFORMED,
    TOTAL_ACCEPTED
}


public class DataCleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private Map<String, String> userIdMap = new HashMap<String, String>();
    private Map<String, String> bookIdMap = new HashMap<String, String>();
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());

        // first file - user_id_map.csv
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(files[0].toString()))));

        //let the header line go
        br.readLine();

        String line = null;
        while((line = br.readLine()) != null) {
            String[] vals = line.split(",");
            userIdMap.put(vals[0], vals[1]);
        }
        br.close();

        // second file - book_id_map.csv
        br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(files[1].toString()))));

        //let the header line go
        br.readLine();

        line = null;
        while((line = br.readLine()) != null) {
            String[] vals = line.split(",");
            bookIdMap.put(vals[0], vals[1]);
        }
        br.close();
    }

    @Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //ignonre the header line
        if(!value.toString().contains("user_id")) {
            //user_id,book_id,is_read,rating,is_reviewed
            String[] vals = value.toString().split(",");

            //only add the lines where is_read is 1
            if(vals[2].equals("1")) {

                if(userIdMap.containsKey(vals[0]) && bookIdMap.containsKey(vals[1])) {
                    String output = userIdMap.get(vals[0]) + "," + bookIdMap.get(vals[1]) + "," + vals[2] + "," + vals[3] + "," + vals[4];
                    context.write(NullWritable.get(), new Text(output));
                    
                    //increment the total number of records accepted
                    context.getCounter(Records.TOTAL_ACCEPTED).increment(1);
                }
                else {
                    //else the record is malformed. Such records are discarded.
                    context.getCounter(Records.MALFORMED).increment(1);
                }
            }
            else {
                //else increment the IS_READ = 0 counter. Such records are discarded.
                context.getCounter(Records.IS_READ_FALSE).increment(1);
            }
        }
    }
}


//Records
//  IS_READ_FALSE=116517139
//  TOTAL_ACCEPTED=112131203


//  MALFORMED=0