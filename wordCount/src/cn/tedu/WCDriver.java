package cn.tedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class WCDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//创建job对象，指定conf和job名称
		Job job = Job.getInstance(conf,"WCJob");
		//指定job执行的类
		job.setJarByClass(WCDriver.class);
		//指定mapper类
		job.setMapperClass(WCMapper.class);
		//指定reducer类
		job.setReducerClass(WCReducer.class);
		
		//设置mapper输出的key和value的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		//设置reducer输出的key和value的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//指定任务操作的资源的位置
		FileInputFormat.setInputPaths(job,
				new Path("hdfs://192.168.80.98:9000/park/word"));
		//指定任务操作结束后生产的结果文件保存的位置
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.80.98:9000/park/word/result"));
		//执行任务
		job.waitForCompletion(true);
	}
}
