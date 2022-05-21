import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.NullWritable;


public class Project {

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println("Usage: Project <input path> <output path>");
      System.exit(-1);
    }
    
    //first job for authors dataset
    Job job = new Job();
    job.setNumReduceTasks(1);
    job.setJarByClass(Project.class);
    job.setJobName("Project");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    
    job.setMapperClass(ProjectMapper.class);
    job.setReducerClass(ProjectReducer.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    //second job for genres dataset
    Job job2 = new Job();
    job2.setNumReduceTasks(1);
    job2.setJarByClass(Project.class);
    job2.setJobName("Project2");

    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[3]));
    
    job2.setMapperClass(ProjectMapper2.class);
    job2.setReducerClass(ProjectReducer2.class);

    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(Text.class);
    
    System.exit(job.waitForCompletion(true) && job2.waitForCompletion(true) ? 0 : 1);
  }
}