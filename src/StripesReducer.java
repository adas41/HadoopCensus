import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class StripesReducer extends MapReduceBase implements Reducer<Text, MapWritable, Text, MapWritable>{
	private final static IntWritable one = new IntWritable(1);
	
	public void reduce(Text key, Iterator<MapWritable> values, OutputCollector<Text, MapWritable> output,
			 Reporter reporter) throws IOException {
		// replace KeyType with the real type of your key
		
		MapWritable newResult = new MapWritable();
		 while (values.hasNext())
		 {
			MapWritable resultSet = values.next();
			
			
			for( Writable current : resultSet.keySet()){
				if(newResult.containsKey(current)){
					int count = Integer.parseInt(newResult.get(current).toString()) + 1;
					newResult.put(current, new Text(String.valueOf(count)));
				}else{
					newResult.put(current,new Text("1"));
				}
			}
			
			System.out.println(resultSet);	
			
		 }
		 output.collect(new Text(key),newResult); 
		 
		 }
		
}


