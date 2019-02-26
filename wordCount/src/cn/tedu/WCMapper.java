package cn.tedu;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**Mapper<...>
LongWritable, 输入key类型 偏移量 
Text,  输入value类型  读取到的一行文本内容
Text, 输出key类型   hello或tom   一个单词
LongWritable：输出value类型   单词的个数
words.txt源文件内容
 *  hello tom
	hello joy
	hello rose
	hello joy
	hello jerry
   每行内容调用一次map方法。
  Mapper读取到的内容：
	 0 hello tom
	 11 hello joy
	 22 hello rose
	 34 hello joy
	 45 hello jerry
   输出内容：
   hello 1,joy 1,
   hello 1,rose 1,
   hello 1,joy 1,
   hello 1,tom 1,
   hello 1,jerry 1, 
 * @author jinxf
 */
public class WCMapper extends Mapper<LongWritable, 
Text, Text, LongWritable> {
	/**LongWritable key, Text value, 输入的key和value
	 * Mapper<LongWritable, Text, Text, LongWritable>.Context context
	 * 向外输出的对象，输出的key和value的类型看第三和第四个参数
	 */
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		//获取读取到一行文本内容
		String line = value.toString();
		System.out.println("map_in:"+key.get()+" "+line);
		//使用空格拆分
		System.out.print("map_out:");
		String words[] = line.split(" ");
		for (String word : words) {
			System.out.print(word+" 1,");
			context.write(new Text(word), new LongWritable(1));
		}
		System.out.println();
	}
}
