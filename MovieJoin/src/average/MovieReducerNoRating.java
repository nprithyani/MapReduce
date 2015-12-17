package average;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MovieReducerNoRating extends MapReduceBase implements
		Reducer<IntWritable, Text, Text, Text> {

	private static final Text EMPTY_TEXT = new Text("");
	
	private Text tmp = new Text();
	private ArrayList<Text> listA = new ArrayList<Text>();
	private ArrayList<Text> listB = new ArrayList<Text>();
	
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		// Clear our lists
		listA.clear();
		listB.clear();
		// iterate through all our values, binning each record based on what
		// it was tagged with. Make sure to remove the tag!
		while (values.hasNext()) {
			tmp = values.next();
			if (tmp.charAt(0) == 'A') {
				listA.add(new Text(tmp.toString().substring(1)));
			} 
			else if (tmp.charAt(0) == 'B') {
				listB.add(new Text(tmp.toString().substring(1)));
			}
		}
		
		if (!listA.isEmpty() && listB.isEmpty()) {
			for (Text A : listA) {
					output.collect(A, EMPTY_TEXT);
				}
			}
		
			
		}

		
	}
