import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class StripesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, MapWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	

	public void map(LongWritable key, Text value, OutputCollector<Text, MapWritable> output, Reporter reporter){
		String line = value.toString();
		
		if(!line.trim().equals("")){
			String[] record = line.split(",");
			String age = record[0].trim();
			String occ = record[6].trim();
			//Map<Text, IntWritable> result = new HashMap<Text, IntWritable>();
			//result.put(new Text(occ), one);
			MapWritable result = new MapWritable();
			result.put(new Text(occ), one);
			
			try {
				output.collect(new Text(age), result);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	

}
