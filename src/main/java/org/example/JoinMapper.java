/**
 * projectName: MapReduce-Join
 * fileName: JoinMapper.java
 * packageName: org.example
 * date: 2024-01-10 15:59
 */
package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author: QSky
 * @data: 2024-01-10 15:59
 **/
public class JoinMapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String name = ((FileSplit)context.getInputSplit()).getPath().getName();
        String [] str = value.toString().split("\\t");
        if (name.equals("a.txt")) {
            context.write(new Text(str[0]), new Text(value +"\ta.txt"));
        }else {
            context.write(new Text(str[0]),new Text(value + "\tb.txt"));
        }
    }
}