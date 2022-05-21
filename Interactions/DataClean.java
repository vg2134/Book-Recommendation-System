import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;

public class DataClean {
    public static void main(String[] args) throws Exception {

        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: DataClean <input path> <output path> [comma separated distributed cache files]");
            System.exit(-1);
        }
        
        Job job = Job.getInstance();
    
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if(args.length == 3) {
            String[] files = args[2].split(",");
            for(String file : files) {
                DistributedCache.addCacheFile(new Path(file).toUri(), job.getConfiguration());
            }
        }

        job.setNumReduceTasks(1);
        job.setJarByClass(DataClean.class);
        job.setJobName("Data Cleaning");
        job.setMapperClass(DataCleanMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
    
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
