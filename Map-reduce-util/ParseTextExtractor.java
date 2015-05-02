package org.apache.nutch.jeet;

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

public class ParseTextExtractor implements Mapper<WritableComparable, Writable, Text, CommonWritable>, Reducer<Text, CommonWritable, Text, CommonWritable> {
	
	public static final Logger LOG = LoggerFactory.getLogger(ParseTextExtractor.class);
	private Text newKey = new Text();
	private JobConf jobConf;
	
	public void map(WritableComparable key, Writable writable,
			OutputCollector<Text, CommonWritable> output, Reporter reporter)
					throws IOException {
		if(writable instanceof Content) {
			try{
			Map<String, List<String>> map = processContent(key, (Content)writable, reporter);
			Iterator<Entry<String, List<String>>> iter = map.entrySet().iterator();
			Content content = (Content) writable;			
			String cknid = content.getMetadata().get("ck_nid").toString();
		
			while(iter.hasNext()) {
				Entry<String, List<String>> entry = iter.next();
				String ck_nid = entry.getKey();
				List<String> parseText = entry.getValue();
				String[] array = parseText.toArray(new String[parseText.size()]);
				ObjectWritable ow = new ObjectWritable(array);
				try {
					output.collect(new Text(ck_nid), new CommonWritable(ow));	
				} catch(Throwable t) {
					System.out.println("Exception caught..."+ t);
				}
				
			}
			}
			catch(Exception e){
				LOG.error("Error while processing "+key  + "  " + e);
			}
		}
		
	}
	
	public void reduce(Text key, Iterator<CommonWritable> values,
			OutputCollector<Text, CommonWritable> output, Reporter reporter)
					throws IOException {
		
		Set<String> urls = new HashSet<String>();
		Set<String> name = new HashSet<String>();
		
		while(values.hasNext()) {
			Writable value = values.next().get();	
			if(value instanceof ObjectWritable) {
				ObjectWritable ow = (ObjectWritable)value;
				
				output.collect(key,new CommonWritable(ow));
				
			} 
		}
		
					
	}

	private Map<String, List<String>> processContent(WritableComparable key, Content content, Reporter reporter) throws IOException {

		Map<String , List<String>> companyIdURLMapping = new HashMap<String , List<String>>();

	
		if (key instanceof Text) {
			newKey.set(key.toString());
			key = newKey;
		}
		int status =
				Integer.parseInt(content.getMetadata().get(Nutch.FETCH_STATUS_KEY));
		if (status != CrawlDatum.STATUS_FETCH_SUCCESS) {
			// content not fetched successfully, skip document
			LOG.debug("Skipping " + key + " as content is not fetched successfully");
			return companyIdURLMapping;
		}

		try {
			ParseResult parseResult = new ParseUtil(jobConf).parse(content);
			for (Entry<Text, Parse> entry : parseResult) {
				Text url = entry.getKey();
				Parse parse = entry.getValue();
				ParseStatus parseStatus = parse.getData().getStatus();

				long start = System.currentTimeMillis();

				reporter.incrCounter("ParserStatus", ParseStatus.majorCodes[parseStatus.getMajorCode()], 1);

				if (!parseStatus.isSuccess()) {
					LOG.warn("Error parsing: " + key + ": " + parseStatus);
				} else {
					String ck_nid = parse.getData().getMeta("ck_nid");
				
							List<String> list = companyIdURLMapping.get(ck_nid);
							if(list == null)
								list = new ArrayList<String>();
							list.add(url+"#$#"+parse.getText());
							companyIdURLMapping.put(ck_nid, list);
						
					
					long end = System.currentTimeMillis();
					LOG.info("Parsed (" + Long.toString(end - start) + "ms):" + url);

				}
			}
		} catch (Exception e) {
			LOG.warn("Error parsing: " + key + ": " + StringUtils.stringifyException(e));
		}
		return companyIdURLMapping;
	}

	public void configure(JobConf job) {
		jobConf = job;
	}
	public void close() throws IOException {}  
}
