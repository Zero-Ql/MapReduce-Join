/**
 * projectName: MapReduce-Join
 * fileName: Join.java
 * packageName: org.example
 * date: 2024-01-10 15:59
 */
package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: QSky
 * @data: 2024-01-10 15:59
 **/
public class Join {
    private static Configuration configuration = new Configuration();

    public static void main(String[] args) {
        configuration.set("fs.default", "hdfs://192.168.234.128:9000");
        Path outputpath = new Path(args[1]);
        try {
            Job job = Job.getInstance(configuration);

            FileSystem fs = FileSystem.get(configuration);

            job.setJarByClass(Join.class);

            job.setMapperClass(JoinMapper.class);
            job.setReducerClass(JoinReducer.class);

//            job.setCombinerClass(JoinReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(JoinWritable.class);

            FileInputFormat.setInputPaths(job, new Path(args[0]));
            if (fs.exists(outputpath)) {
                fs.delete(outputpath);
            }
            FileOutputFormat.setOutputPath(job, outputpath);

            job.waitForCompletion(true);
        } catch (ClassNotFoundException | InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}