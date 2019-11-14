package test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.io.IOException;

public class MyRecordReader extends RecordReader {
    private FileSplit fileSplit = null;
    private FSDataInputStream fsDataInputStream = null;
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();
    private boolean isReaded = false;

    //从split获取到文件路径，开启一个流
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //1,将inputSplit转化为FileSplit
        fileSplit = (FileSplit) split;
        //2，从fileSplit获取到包含这个split的源文件的位置
        Path fileSplitPath = fileSplit.getPath();
        //3，通过FileSystem对象，打开这个split所在的文件的stream（FSDataInputStream）
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        fsDataInputStream = fileSystem.open(fileSplitPath);
    }

    //从split中获取到路径名和文件名作为key
    //从流中获取到二进制的文件作为value
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isReaded == false) {
            String path = fileSplit.getPath().toString();
            key.set(path);

            byte[] bytes = new byte[(int) fileSplit.getLength()];
            fsDataInputStream.read(bytes);
            value.set(bytes, 0, (int) fileSplit.getLength());
            isReaded = true;
            return true;
        } else {
            return false;
        }
    }

    //返回key
    public Object getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    //返回value
    public Object getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    //返回任务进度
    public float getProgress() throws IOException, InterruptedException {
        if (isReaded == false) {
            return 0;
        } else {
            return 1;
        }
    }

    //通过IOUtil来关闭FSDataInputStream
    public void close() throws IOException {
        IOUtils.closeStream(fsDataInputStream);
    }
}
