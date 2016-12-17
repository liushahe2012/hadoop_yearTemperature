package com.hadoop;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunJob {

	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	static class MapperJob extends Mapper<LongWritable, Text, keyPair, Text>
	{

		@Override
		protected void map(LongWritable key, Text value, 
				Context context)
				throws IOException, InterruptedException {

			//处理读取的一行数据
			String line = value.toString();
			//每一行数据由制表符分割，所以需要分割
			String[] ss = line.split("\t");
			
			System.out.println("value=" + value.toString());
			System.out.println("ss.length=" + ss.length);
			//只处理符合条件的数据
			if (ss.length == 2) {
				try {
					//解析年份
					Date date = sdf.parse(ss[0]);
					Calendar c = Calendar.getInstance();
					c.setTime(date);
					//c.get(1)就是年份
					int year = c.get(1);
					System.out.println("year=" + year);
					//解析温度
					String t = ss[1].substring(0, ss[1].indexOf("C"));
					System.out.println("t=" + t);
					
					//创建复合对象键值keyPair
					keyPair k = new keyPair();
					k.setYear(year);
					k.setTemperature(Integer.parseInt(t));
					
					Text t1 = new Text(t);
					System.out.println("t1=" + t1);
					//mapper写输出
					context.write(k, t1);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		}
	}
	
	static class ReducerJob extends Reducer<keyPair, Text, keyPair, Text>
	{
		
		protected void reduce(keyPair key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			//直接输出
			for(Text v: value)
			{
				context.write(key, v);
			}
		}
		
	}
	
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try
		{
			Job job = new Job(conf);
			job.setJobName("year_temperature");
			job.setJarByClass(RunJob.class);
			job.setMapperClass(MapperJob.class);
			job.setReducerClass(ReducerJob.class);
			job.setMapOutputKeyClass(keyPair.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setNumReduceTasks(3);
			job.setPartitionerClass(Partition.class);
			job.setSortComparatorClass(Sort.class);
			job.setGroupingComparatorClass(Group.class);
			
			//输入输出文件路径
			FileInputFormat.addInputPath(job, new Path("/usr/local/hadooptempdata/input/year-temp/"));
			FileOutputFormat.setOutputPath(job, new Path("/usr/local/hadooptempdata/output/year-temp/"));
			System.exit(job.waitForCompletion(true)? 0 : 1);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
