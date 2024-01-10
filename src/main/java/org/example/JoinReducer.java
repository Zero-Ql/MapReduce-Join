/**
 * projectName: MapReduce-Join
 * fileName: JoinReducer.java
 * packageName: org.example
 * date: 2024-01-10 15:59
 */
package org.example;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: QSky
 * @data: 2024-01-10 15:59
 **/
public class JoinReducer extends Reducer<Text, Text, NullWritable, JoinWritable> {
    private String[] str;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        JoinWritable joinWritable = new JoinWritable();
        values.forEach((value) -> {
            str = value.toString().split("\\t");
            if (str[str.length - 1].equals("a.txt")) {
                joinWritable.setOrderId(Long.parseLong(str[0]));
                joinWritable.setAccount(str[1]);
                joinWritable.setDate(str[2]);
            } else {
                joinWritable.setItemId(Long.parseLong(str[1]));
                joinWritable.setNum(Long.parseLong(str[2]));
            }
        });
        context.write(NullWritable.get(), joinWritable);
    }
}