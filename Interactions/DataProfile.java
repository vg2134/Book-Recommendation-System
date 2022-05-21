import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataProfile {

    public static void main(String[] args) throws Exception {
        
        if (args.length != 2) {
            System.err.println("Usage: DataProfile <input path> <output path>");
            System.exit(-1);
        }

         
        Job job = Job.getInstance();
    
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(1);
        job.setJarByClass(DataProfile.class);
        job.setJobName("Data Profiling");
        job.setMapperClass(DataProfileMapper.class);
        job.setReducerClass(DataProfileReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
    
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
    
}
