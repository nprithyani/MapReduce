package average;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import util.MovieRecord;

public class MovieReducer extends MapReduceBase implements
		Reducer<IntWritable, MovieRecord, NullWritable, Text> {

	static int i = 0;
	public void reduce(IntWritable key, Iterator<MovieRecord> values,
			OutputCollector<NullWritable, Text> output, Reporter reporter)
			throws IOException {
		
		MovieRecord mr = new MovieRecord();
		
		if (i == 0){
		while (values.hasNext()) {
			if (i == 0){
			mr = values.next();
			output.collect(NullWritable.get(), mr.getMovieName());
			i = i + 1;
			}
		}	
		}
			
		}

		
	}
