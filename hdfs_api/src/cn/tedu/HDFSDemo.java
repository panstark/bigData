package cn.tedu;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
public class HDFSDemo {
	@Test
	public void testConnect() throws Exception{
		Configuration conf = new Configuration();
		//还可以通过conf.set(name,value)指定属性的的值，如果指定了以它为主。
		//conf.set("dfs.replication", "1");
		//如果不指定则使用Configuration默认值
		//FileSystem是Hadoop的文件系统的抽象类，HDFS分布式文件系统只是Hadoop文件系统中的一种，对应的实现类：
		//org.apache.hadoop.hdfs.DistributedFileSystem。HDFS是Hadoop开发者最常用的文件系统，
		//因为HDFS可以和MapReduce结合，从而达到处理海量数据的目的
		//Hftp：基于HTTP方式去访问HDFS文件提供（只读）
		//hdfs:可以读取、写入、改等操作。
		//get(URI:制定位置"ip:9000",conf)
		FileSystem fs = FileSystem.get(
				new URI("hdfs://192.168.80.98:9000"), conf);
		System.out.println(fs);
		//关闭
		fs.close();
	}
	@Test
	public void testMkdir() throws Exception, URISyntaxException{
		//get(URI,Conf..,"User")
		FileSystem fs = FileSystem.get(
				new URI("hdfs://192.168.80.98:9000"), 
				new Configuration(),"root");
		//创建目录
		fs.mkdirs(new Path("/park4"));
		fs.close();
	}
	//从hdfs上下载文件到本地
	@Test
	public void testCopyToLocal() throws Exception, InterruptedException, URISyntaxException{
		FileSystem fs = FileSystem.get(
				new URI("hdfs://192.168.80.98:9000"), 
				new Configuration(),"root");
		//FSDataInputStream是InputStream类的后代类
		FSDataInputStream fsin = fs.open(new Path("/park1/1.txt"));
		OutputStream out = new FileOutputStream(
				new File("1.txt"));
		/*int len = -1;
		byte[] bts = new byte[1024];
		while((len=fsin.read(bts))!=-1){
			out.write(bts, 0, len);
		}*/
		IOUtils.copy(fsin, out, 1024);
		out.close();
		fsin.close();
		fs.close();
	}
}
