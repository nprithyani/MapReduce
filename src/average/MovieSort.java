package average;

// cc MaxTemperature Application to find the maximum temperature in the weather dataset
// vv MaxTemperature
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import util.MovieRecord;

public class MovieSort {

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: MovieSort <input path> <output path>");
      System.exit(-1);
    }
    
    JobConf conf = new JobConf(MovieSort.class);
    conf.setJobName("Movie Sort");

    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setMapperClass(MovieMapper.class);
    conf.setReducerClass(MovieReducer.class);
    
    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(MovieRecord.class);
    
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(Text.class);

    JobClient.runJob(conf);
  }
}
