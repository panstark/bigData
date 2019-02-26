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
		//������ͨ��conf.set(name,value)ָ�����Եĵ�ֵ�����ָ��������Ϊ����
		//conf.set("dfs.replication", "1");
		//�����ָ����ʹ��ConfigurationĬ��ֵ
		//FileSystem��Hadoop���ļ�ϵͳ�ĳ����࣬HDFS�ֲ�ʽ�ļ�ϵͳֻ��Hadoop�ļ�ϵͳ�е�һ�֣���Ӧ��ʵ���ࣺ
		//org.apache.hadoop.hdfs.DistributedFileSystem��HDFS��Hadoop��������õ��ļ�ϵͳ��
		//��ΪHDFS���Ժ�MapReduce��ϣ��Ӷ��ﵽ���������ݵ�Ŀ��
		//Hftp������HTTP��ʽȥ����HDFS�ļ��ṩ��ֻ����
		//hdfs:���Զ�ȡ��д�롢�ĵȲ�����
		//get(URI:�ƶ�λ��"ip:9000",conf)
		FileSystem fs = FileSystem.get(
				new URI("hdfs://192.168.80.98:9000"), conf);
		System.out.println(fs);
		//�ر�
		fs.close();
	}
	@Test
	public void testMkdir() throws Exception, URISyntaxException{
		//get(URI,Conf..,"User")
		FileSystem fs = FileSystem.get(
				new URI("hdfs://192.168.80.98:9000"), 
				new Configuration(),"root");
		//����Ŀ¼
		fs.mkdirs(new Path("/park4"));
		fs.close();
	}
	//��hdfs�������ļ�������
	@Test
	public void testCopyToLocal() throws Exception, InterruptedException, URISyntaxException{
		FileSystem fs = FileSystem.get(
				new URI("hdfs://192.168.80.98:9000"), 
				new Configuration(),"root");
		//FSDataInputStream��InputStream��ĺ����
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
