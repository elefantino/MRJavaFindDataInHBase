//Copyright (c) 2015 Elena Rose
//
//Permission is hereby granted, free of charge, to any person obtaining a copy
//of this software and associated documentation files (the "Software"), to deal
//in the Software without restriction, including without limitation the rights
//to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//copies of the Software, and to permit persons to whom the Software is
//furnished to do so, subject to the following conditions:
//
//The above copyright notice and this permission notice shall be included in
//all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//THE SOFTWARE.

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

public class DataHBase {
	
	//Map class
	static class TableMap extends TableMapper<Text, Text> {
		
		protected void map(ImmutableBytesWritable rowkey, Result value, Context context) 
				throws IOException, InterruptedException {

        	HashMap<String, String> vc = new HashMap<String, String>();
			vc.put("package_time", "");
			vc.put("location_name", "");
			vc.put("sequence_number", "");
			vc.put("tick_count", "");
			vc.put("signal_states", "");
			vc.put("detector_states", "");
			vc.put("detector_levels", "");
			
			KeyValue[] keyValue= value.raw();
			for(int i=0; i < keyValue.length; i++) {
				vc.put(Bytes.toString(keyValue[i].getQualifier()),Bytes.toString(keyValue[i].getValue()));
			}
			
			if (vc.get("package_time").equals("") || vc.get("tick_count").equals("")) {}
			else {
				
				//The key is a string "package_time,location_name,sequence_number" 
				String myKey = vc.get("package_time") + "," + vc.get("location_name") + "," + 
					vc.get("sequence_number");
			
				//The value is a string "tick_count,signal_states,detector_states,detector_levels" 
				String myValue =  vc.get("tick_count") + "," + vc.get("signal_states") + "," + 
						vc.get("detector_states") + "," + vc.get("detector_levels");
				
				//write the result
				context.write(new Text(myKey), new Text(myValue));
				
			}
			
        }
		
    }
	
	
	//Reduce class
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			
			for (Text val : values) {
				context.write(key, val);
			}	
			    
		}
	}
	
		
	public void run(HashMap<String, String> config) throws Exception {
				
		//clean the former output if it exists
		Path p = new Path(config.get("hdfs_output_dir"));
		FileSystem fs = FileSystem.get(new Configuration());
	    if (fs.exists(p)) {
	    	fs.delete(p, true);
	    }
	    
	    String junction = config.get("what_to_find"); // the name of the junction
		String date1 = config.get("date1");
		String date2 = config.get("date2");
		//date1 and date2 can be of a format YYYY-MM-DD
		if (date1.length() == 10)
			date1 = date1 + " 00:00:00";
		if (date2.length() == 10)
			date2 = date2 + " 23:59:59";
		System.out.println("Looking for data of " + junction + ": " + date1 + " - " + date2);
		
		//create timestamps (considering time zone!) to limit data
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		sdf.setTimeZone(TimeZone.getDefault());
		Long time1 = sdf.parse(date1).getTime();
		Long time2 = sdf.parse(date2).getTime();
		
        //run a job
		Configuration conf = HBaseConfiguration.create();
		conf.set("mapreduce.output.textoutputformat.separator", ","); //set comma as a delimiter
		
		Job job = new Job(conf, "Retrieve data from hbase");
		job.setJarByClass(DataHBase.class);
		
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		scan.setMaxVersions(1);
		scan.setTimeRange(time1, time2); //take a day we are interested in
		//set a filter for a junction name
		if (!junction.equals("")) {
			SingleColumnValueFilter filter = new SingleColumnValueFilter(
					Bytes.toBytes("data"),
					Bytes.toBytes("location_name"),
					CompareOp.EQUAL,
					Bytes.toBytes(junction)
					);
			scan.setFilter(filter);
		}	
		//add the specific columns to the output to limit the amount of data
		scan.addFamily(Bytes.toBytes("data"));
		
		TableMapReduceUtil.initTableMapperJob(
				config.get("hbase_table"),	// input HBase table name
				scan,             			// Scan instance to control CF and attribute selection
				TableMap.class,   			// mapper
				Text.class,             	// mapper output key
				Text.class,   	// mapper output value
				job);
		
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(config.get("hdfs_output_dir")));
		
		job.waitForCompletion(true);
								
	}
	
}	