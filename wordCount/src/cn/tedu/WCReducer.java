package cn.tedu;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**Reducer<Text,LongWritable, Text, LongWritable>
 * 第一个参数：输入key的类型
 * 第二参数：输入value集合中元素的类型
 * 第三个参数：输出key类型
 * 第四个参数：输入value类型
Reducer输入内容
	hello 1,1,1,1,1,1,1,1
	joy   1,1,1
	jerry 1
Reducer输出内容：
	hello 8
	jerry 1
	joy   3
 * @author jinxf
 */
public class WCReducer extends Reducer<Text,
LongWritable, Text, LongWritable> {
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		Iterator<LongWritable> vals = values.iterator();
		long count = 0;
		/*while(vals.hasNext()){
			count += vals.next().get();
		}*/
		String inval = "";
		while(vals.hasNext()){
			long val = vals.next().get();
			count+=val;
			inval=inval+","+val;
		}
		System.out.println("reduce_in:"+key.toString()+" "+inval);
		context.write(key, new LongWritable(count));
		System.out.println("reduce_out:"+key.toString()+" "+count);
	}
}
