
package org.apache.nutch.jeet;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.GenericWritableConfigurable;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Parse content in a segment. */
public class ExecValidator extends Configured implements Tool {

	public static final Logger LOG = LoggerFactory.getLogger(ExecValidator.class);
	public static final Logger log = LoggerFactory.getLogger(DumpProcessor.class);

	public static final String SKIP_TRUNCATED = "parser.skip.truncated";
	private static JobConf jobConf;
	
	private ScoringFilters scfilters;

	private boolean skipTruncated;
	
	public void configure(JobConf job) {
		setConf(job);
		this.jobConf = job;
		this.scfilters = new ScoringFilters(job);
		skipTruncated=job.getBoolean(SKIP_TRUNCATED, true);
	}

	public void close() {}



	/**
	 * Checks if the page's content is truncated.
	 * @param content
	 * @return If the page is truncated <code>true</code>. When it is not,
	 * or when it could be determined, <code>false</code>. 
	 */
	public static boolean isTruncated(Content content) {
		byte[] contentBytes = content.getContent();
		if (contentBytes == null) return false;
		Metadata metadata = content.getMetadata();
		if (metadata == null) return false;

		String lengthStr = metadata.get(Response.CONTENT_LENGTH);
		if (lengthStr != null) lengthStr=lengthStr.trim();
		if (StringUtil.isEmpty(lengthStr)) {
			return false;
		}
		int inHeaderSize;
		String url = content.getUrl();
		try {
			inHeaderSize = Integer.parseInt(lengthStr);
		} catch (NumberFormatException e) {
			LOG.warn("Wrong contentlength format for " + url, e);
			return false;
		}
		int actualSize = contentBytes.length;
		if (inHeaderSize > actualSize) {
			LOG.info(url + " skipped. Content of size " + inHeaderSize
					+ " was truncated to " + actualSize);
			return true;
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug(url + " actualSize=" + actualSize + " inHeaderSize=" + inHeaderSize);
		}
		return false;
	}



	public void parse(Path dump, ArrayList<Path> segment, Path nameDir) throws IOException {
	Path tmpDir = new Path("tmp-"+System.currentTimeMillis());
	Path tmpDir1 = new Path(tmpDir,"tmp-"+System.currentTimeMillis());
	
	processDumpFile(dump, tmpDir1);
		
		Path tmpDir2 = new Path(tmpDir,"tmp-"+System.currentTimeMillis());
		extractParseText(segment, tmpDir2);
		
		Configuration conf = getConf();
		final FileSystem fs = FileSystem.get(conf);
		ArrayList<Path> out = new ArrayList<Path>();
		FileStatus[] paths = fs.listStatus(tmpDir, HadoopFSUtil.getPassDirectoriesFilter(fs));
        out.addAll(Arrays.asList(HadoopFSUtil.getPaths(paths)));
		
		
	        
        validateUrl(out,nameDir);
		
	}


	private void validateUrl(ArrayList<Path> output, Path nameDir) throws IOException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = System.currentTimeMillis();
		if (LOG.isInfoEnabled()) {
			LOG.info("URLValidator: starting at " + sdf.format(start));
			
		}

		JobConf urlValidator = new NutchJob(getConf());
		urlValidator.setJobName("URLValidator");
		
		for(Path path1:output){
			FileInputFormat.addInputPath(urlValidator, path1);
			System.out.println("added "+path1.toString());
		}
		
		


		urlValidator.setInputFormat(SequenceFileInputFormat.class);
		
		urlValidator.setMapperClass(URLValidator.class);
		urlValidator.setReducerClass(URLValidator.class);

		
		FileOutputFormat.setOutputPath(urlValidator, new Path(nameDir.toString()));
		
		
		urlValidator.setMapOutputKeyClass(Text.class);
		urlValidator.setMapOutputValueClass(CommonWritable.class);
		
		urlValidator.setOutputKeyClass(Text.class);
		urlValidator.setOutputValueClass(Text.class);
		
		JobClient.runJob(urlValidator);
		long end = System.currentTimeMillis();
		LOG.info("URLValidator: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
		
	}

	private void processDumpFile(Path nameDir, Path tmpDir) throws IOException {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = System.currentTimeMillis();
		if (LOG.isInfoEnabled()) {
			LOG.info("ExecExtractor: starting at " + sdf.format(start));
		
		}

		JobConf dumpFileProcessor = new NutchJob(getConf());
		FileInputFormat.addInputPath(dumpFileProcessor, nameDir);
		
		dumpFileProcessor.setMapperClass(DumpProcessor.class);
		dumpFileProcessor.setReducerClass(DumpProcessor.class);
		FileOutputFormat.setOutputPath(dumpFileProcessor, tmpDir);
		dumpFileProcessor.setMapOutputKeyClass(Text.class);
		dumpFileProcessor.setMapOutputValueClass(CommonWritable.class);
		
		dumpFileProcessor.setOutputKeyClass(Text.class);
		dumpFileProcessor.setOutputValueClass(CommonWritable.class);
		dumpFileProcessor.setOutputFormat(SequenceFileOutputFormat.class);
		
		JobClient.runJob(dumpFileProcessor);
		long end = System.currentTimeMillis();
		LOG.info("ExecExtractor: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
	}

	private void extractParseText(ArrayList<Path> segment, Path nameDir) throws IOException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = System.currentTimeMillis();
		if (LOG.isInfoEnabled()) {
			LOG.info("ParseTextExtractor: starting at " + sdf.format(start));
			LOG.info("ParseTextExtractor: segment: " + segment);
		}

		JobConf parseTextExtractor = new NutchJob(getConf());
		parseTextExtractor.setJobName("ParseTextEXtractor");
		
		for(Path path1:segment){
			FileInputFormat.addInputPath(parseTextExtractor, new Path(path1, Content.DIR_NAME));
		}
		
		


		parseTextExtractor.setInputFormat(SequenceFileInputFormat.class);
		
		parseTextExtractor.setMapperClass(ParseTextExtractor.class);
		parseTextExtractor.setReducerClass(ParseTextExtractor.class);

		
		FileOutputFormat.setOutputPath(parseTextExtractor, new Path(nameDir.toString()));
		
		
		parseTextExtractor.setMapOutputKeyClass(Text.class);
		parseTextExtractor.setMapOutputValueClass(CommonWritable.class);
		
		parseTextExtractor.setOutputKeyClass(Text.class);
		parseTextExtractor.setOutputValueClass(CommonWritable.class);
		parseTextExtractor.setOutputFormat(SequenceFileOutputFormat.class);
		JobClient.runJob(parseTextExtractor);
		long end = System.currentTimeMillis();
		LOG.info("ParseTextExtractor: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
		
	}

	public static void main(String[] args) throws Exception {
		// args[0]=dump directory
		// args[1]=segments directory
		// args[2]=output directory
		
		Configuration conf = NutchConfiguration.create();
		int res = ToolRunner.run(conf, new ExecValidator(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		String usage = "Usage: ExecValidator dump_dir segments_dir output_dir";
		Configuration conf = getConf();
		final FileSystem fs = FileSystem.get(conf);
		if (args.length == 0) {
			System.err.println(usage);
			System.exit(-1);
		}
		Path dump = new Path(args[0]);
		Path segment = new Path(args[1]);
		Path nameDir = new Path(args[2]);
		ArrayList<Path> segs = new ArrayList<Path>();
		FileStatus[] paths = fs.listStatus(segment, HadoopFSUtil.getPassDirectoriesFilter(fs));
        segs.addAll(Arrays.asList(HadoopFSUtil.getPaths(paths)));
		parse(dump,segs, nameDir);
		return 0;
	}
	
	private static class DumpProcessor implements Mapper<WritableComparable, Writable, Text, CommonWritable>, Reducer<Text, CommonWritable, Text, CommonWritable> {
	 static final Logger logger = LoggerFactory.getLogger(DumpProcessor.class);
	 public void map(WritableComparable key, Writable writable,
				OutputCollector<Text, CommonWritable> output, Reporter reporter)
						throws IOException {
			Text text = (Text)writable;
			String line = text.toString();
			
			
			
			String[] arr=line.split("\t");
			
			String ck_nid = arr[4];
			
		
			
			
			String str=arr[0]+"#$#"+arr[5];
			
	
		output.collect(new Text(ck_nid.replaceAll("\"","")), new CommonWritable(new Text(str)));
			
		}
	
	 public void reduce(Text key, Iterator<CommonWritable> values,
				OutputCollector<Text, CommonWritable> output, Reporter reporter)
						throws IOException {
		
		 while(values.hasNext()) {
				Writable value = ( values.next().get());	
				if(value instanceof Text) {
					Text ow = (Text)value;
				
					
					LOG.info("Collecting "+key + ow.toString());
				output.collect(key,new CommonWritable(ow));
				
				}
			}
		}
		public void configure(JobConf job) {}
		public void close() throws IOException {}  
	}
}
