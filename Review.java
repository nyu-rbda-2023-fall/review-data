import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Review {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: Review <input path> <output path>");
      System.exit(-1);
    }
    Job job = Job.getInstance();
    job.setJarByClass(Review.class);
    job.setJobName("Review");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    //job.setCombinerClass(ReviewReducer.class);
    job.setMapperClass(ReviewMapper.class);
    //job.setReducerClass(ReviewReducer.class);

    // job.setNumReduceTasks(1);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(ReviewDataWritable.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
