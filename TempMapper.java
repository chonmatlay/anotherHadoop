package MeanTemp;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper ; 

public class TempMapper extends  Mapper <Object,Text, Text,FloatWritable>{
    public void map(Object key, Text value , Context context) throws IOException, InterruptedException{
        String  valueString = value.toString();
        String[] TempData= valueString.split(",");
        FloatWritable temp = new FloatWritable(Float.parseFloat(TempData[2]));
        String date= TempData[0];
        Text month =new Text(date.substring(2));
        context.write(month,temp);
    }
}