package edu.nyu.cloud;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sorter extends PageRankTool 
{
    @Override
    public int run(String[] args) throws Exception
    {
        Util.print("Sorter.run", args);
        
        Job job = new Job(getConf(), "Page Rank Sorting");
        
        // Config file system
        Path input = new Path(args[0] + "/part-r-00000");  
        Path output = new Path(args[1]); 
        FileOutputFormat.setOutputPath(job, output);
        FileInputFormat.addInputPath(job, input);
        
        // Config job
        job.setJarByClass(Sorter.class);
        job.setMapperClass(SorterMapper.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(DoubleWritableDecreasing.class);
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static class SorterMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>
    {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            StringTokenizer st = new StringTokenizer(value.toString(), PageRankParams.DELIMITER+"");
            Text url = new Text(st.nextToken());
            double rank = Double.parseDouble(st.nextToken());
            context.write(new DoubleWritable(rank), url);
        }
    }

    private static class DoubleWritableDecreasing extends DoubleWritable.Comparator {

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }

    }

}
