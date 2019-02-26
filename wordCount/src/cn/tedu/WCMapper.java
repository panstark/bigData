package cn.tedu;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**Mapper<...>
LongWritable, ����key���� ƫ���� 
Text,  ����value����  ��ȡ����һ���ı�����
Text, ���key����   hello��tom   һ������
LongWritable�����value����   ���ʵĸ���
words.txtԴ�ļ�����
 *  hello tom
	hello joy
	hello rose
	hello joy
	hello jerry
   ÿ�����ݵ���һ��map������
  Mapper��ȡ�������ݣ�
	 0 hello tom
	 11 hello joy
	 22 hello rose
	 34 hello joy
	 45 hello jerry
   ������ݣ�
   hello 1,joy 1,
   hello 1,rose 1,
   hello 1,joy 1,
   hello 1,tom 1,
   hello 1,jerry 1, 
 * @author jinxf
 */
public class WCMapper extends Mapper<LongWritable, 
Text, Text, LongWritable> {
	/**LongWritable key, Text value, �����key��value
	 * Mapper<LongWritable, Text, Text, LongWritable>.Context context
	 * ��������Ķ��������key��value�����Ϳ������͵��ĸ�����
	 */
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		//��ȡ��ȡ��һ���ı�����
		String line = value.toString();
		System.out.println("map_in:"+key.get()+" "+line);
		//ʹ�ÿո���
		System.out.print("map_out:");
		String words[] = line.split(" ");
		for (String word : words) {
			System.out.print(word+" 1,");
			context.write(new Text(word), new LongWritable(1));
		}
		System.out.println();
	}
}
