package MeanTemp; 
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TempReducer extends  Reducer<Text, FloatWritable, Text,FloatWritable> {
    public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {
        float count =0 ;
        float sum =0 ; 
        for (FloatWritable val : values){
            count+=1; 
            sum += val.get();
        }
        FloatWritable mean = new FloatWritable(sum/count);
        context.write(key,mean);
    }
}