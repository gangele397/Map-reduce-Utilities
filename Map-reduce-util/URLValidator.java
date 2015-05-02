package org.apache.nutch.Jeet;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.insideview.utils.IVStringUtils;
import org.apache.nutch.insideview.utils.SocialHandlesUtil;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URLValidator implements Mapper<WritableComparable, Writable, Text, CommonWritable>, Reducer<Text, CommonWritable, Text, Text> {
	
	public static final Logger LOG = LoggerFactory.getLogger(URLValidator.class);
	private Text newKey = new Text();
	private JobConf jobConf;
	
	public void map(WritableComparable key, Writable writable,
			OutputCollector<Text, CommonWritable> output, Reporter reporter)
					throws IOException {
		output.collect(new Text(key.toString()), (CommonWritable)writable);	
	
		
	}
	
	public void reduce(Text key, Iterator<CommonWritable> values,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {
		
		
		
		Set<String> parseText = new HashSet<String>();
		Set<String> executives = new HashSet<String>();
		
		while(values.hasNext()) {
			Writable value = values.next().get();	
			if(value instanceof ObjectWritable) {
				ObjectWritable ow = (ObjectWritable)value;
				Object object = ow.get();
				String [] array = (String[])object;
				for (int i = 0; i < array.length; i++) {
					parseText.add(array[i]);
				}
				
			
				
			} else if (value instanceof Text) {
				executives.add(((Text)value).toString());
			}
			
		}
		
		

		Iterator<String> pIter = parseText.iterator();
		while(pIter.hasNext()) {
			String text = pIter.next();
		
			
			
					Iterator<String> eIter = executives.iterator();
					while(eIter.hasNext()) {
						String exec = eIter.next();

						String[] arr=exec.split("#\\$#");
						String name=arr[1].replaceAll("\"","");
						String vendorId=arr[0].replaceAll("\"","");
						System.out.println(name+" "+vendorId);
						if(text.contains(name)){
						System.out.println(exec+" "+text);
						text="\""+text.split("#\\$#")[0]+"\"";
						name="\""+name+"\"";
						vendorId="\""+vendorId+"\"";
						String str=key.toString();
						str="\""+str+"\"";
							output.collect(new Text(str), new Text(vendorId+"\t"+name+"\t"+text));
							LOG.info(vendorId+"\t"+name+"\t"+text);
						}
						
					
				
			} 
		}
		
					
	}

	

	public void configure(JobConf job) {
		jobConf = job;
	}
	public void close() throws IOException {}  
}
