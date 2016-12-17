package com.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partition extends Partitioner<keyPair, Text>{

	@Override
	//自定义分区方法
	//num是reduce的数量
	public int getPartition(keyPair key, Text value, int num) {
		//安装年份分区，年份保存在key中
		//年份乘以常数，在对num取模
		return (key.getYear() * 127) % num;
	}

	

	

}
