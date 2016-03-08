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

//This program finds data in HBase 

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Main {
	
	public static void main(String[] args) {
		
		String config_file = "parameters.conf";
		HashMap<String, String> config = new HashMap<String, String>();
        config.put("hbase_table", "");		
        config.put("hdfs_output_dir", "");
        config.put("local_output_dir", "");
        config.put("what_to_find", ""); // as an example we are looking for traffic data for a specific junction name
        config.put("date1", "" );
        config.put("date2", "" );
        
	    try {
            BufferedReader in = new BufferedReader(new FileReader(config_file));
            String str;
            while ((str = in.readLine()) != null) {
                if( str.startsWith(";") || str.isEmpty() )
                    continue;
                else {	   
                    int idx = str.indexOf( "=" );
                    String key = str.substring( 0, idx );
                    int idx2 = str.lastIndexOf( "=" );
                    String value = str.substring( idx2+1, str.length() );
                    config.put( key.trim(), value.trim());  
                }
            }
            in.close();
        } 
        catch (IOException e) {
            System.out.println(e);
            return;
        }
	    
	    //map reduce job: main processing is here
	    DataHBase d = new DataHBase();
	    try {
	    	d.run(config);
	    	try {
	    		
	    		//save output
	    		String filePath = "data_";
	    		if (!config.get("what_to_find").equals("")) 
	    			filePath = filePath + config.get("what_to_find") + "_";
	    		filePath = config.get("local_output_dir") + "/" + filePath + 
	    				config.get("date1").replace(":", "").replace(" ", "-") + ".csv";
	    		
	    		FileSystem hdfs = FileSystem.get(new Configuration());
    			hdfs.copyToLocalFile(false, new Path(config.get("hdfs_output_dir")+"/part-r-00000"), 
    					new Path(filePath));
    			
    			// add a header
    			BufferedReader in = new BufferedReader(new FileReader(filePath));
    			String header = "time,location,sequenceNumber,tickCount,signalStates,detectorStates,detectorLevels";
    		    String content = "";
    			while (in.ready()) {
    		      content = content + in.readLine() + System.getProperty("line.separator");
    		    }
    		    in.close();
    		    FileWriter fstream = new FileWriter(filePath);
    		    BufferedWriter out = new BufferedWriter(fstream);
    		    out.write(header + System.getProperty("line.separator") + content);  
    		    out.close();
    	          
    			System.out.println("Output is saved to " + filePath);
    			
	    	} catch (IOException e) {
	    		System.out.println(e);
	    	}
	    } 
	    catch (Exception e) {
	    	System.out.println(e);
	    }
	   
	  	return;

	}
		
}
