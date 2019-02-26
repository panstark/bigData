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
		//����job����ָ��conf��job����
		Job job = Job.getInstance(conf,"WCJob");
		//ָ��jobִ�е���
		job.setJarByClass(WCDriver.class);
		//ָ��mapper��
		job.setMapperClass(WCMapper.class);
		//ָ��reducer��
		job.setReducerClass(WCReducer.class);
		
		//����mapper�����key��value������
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		//����reducer�����key��value������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//ָ�������������Դ��λ��
		FileInputFormat.setInputPaths(job,
				new Path("hdfs://192.168.80.98:9000/park/word"));
		//ָ��������������������Ľ���ļ������λ��
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.80.98:9000/park/word/result"));
		//ִ������
		job.waitForCompletion(true);
	}
}
