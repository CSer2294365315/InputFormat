package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        //original
        //add in debug
        job.setJarByClass(Driver.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);


        job.setInputFormatClass(MyInputFormat.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path("/input"));
        FileOutputFormat.setOutputPath(job,new Path("/output"));

    }
}


//add in master3
//add in master
//add in master2
//add sth in debug2
//add in master line1
//add in debug

//add sth in debug2//add in master line1
////add in debug
//
////add sth in debug2//add in master line1
////add in debug
//
////add sth in debug2//add in master line1
////add in debug
//
////add sth in debug2//add in master line1
////add in debug
//
////add sth in debug2//add in master line1
////add in debug
//
////add sth in debug2//add in master line1
////add in debug
//
////add sth in debug2//add in master line1
////add in debug
//
////add sth in debug2




