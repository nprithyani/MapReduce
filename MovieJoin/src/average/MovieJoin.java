package average;

// cc MaxTemperature Application to find the maximum temperature in the weather dataset
// vv MaxTemperature
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class MovieJoin {

  public static void main(String[] args) throws IOException {
    if (args.length != 3) {
      System.err.println("Usage: MovieJoin <input path1> <input path2> <output path>");
      System.exit(-1);
    }
    
    //output folder name is output
    
    JobConf conf = new JobConf(MovieJoin.class);
    conf.setJobName("Movie Join");
    
    MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class,
    		MovieMapper.class);
    MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class,
    		MovieRatingMapper.class);

    FileOutputFormat.setOutputPath(conf, new Path(args[2]));
    
    conf.setReducerClass(MovieReducer.class);
    
    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(Text.class);
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    JobClient.runJob(conf);
  }
}
