package edu.nyu.cloud;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class Counter extends PageRankTool 
{

    @Override
    public int run(String[] args) throws Exception
    {
        printArgs("Counter.run", args);
        
        Job job = new Job(getConf(), "Page Rank Counting");
        
        // Config i/o
        Path input = new Path(args[0] + PageRank.OUTPUT_FILENAME);  
        FileInputFormat.addInputPath(job, input);
        Path output = new Path(args[1]); 
        FileOutputFormat.setOutputPath(job, output);
        
        // Config job
        job.setJarByClass(Counter.class);
        job.setMapperClass(CounterMapper.class);
        job.setReducerClass(CounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static class CounterMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        private static final Text COUNTER_KEY = new Text("COUNTER");
        private static final Text ONE = new Text("1");
        
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            context.write(COUNTER_KEY, ONE);
        }
    }
    
    public static class CounterReducer extends Reducer<Text, Text, Text, NullWritable>
    {
        @Override @SuppressWarnings("unused")
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            Integer count = 0;
            for(Text v : values)
            {
                count++;
            }
            context.write(new Text(count.toString()), NullWritable.get());
        }
    }
    
    public static void main(String[] args) throws Exception {
        System.exit (
           ToolRunner.run (
               new Configuration(), 
               new Counter(), 
               new String[] { "/Users/rshepherd/Documents/nyu/cloud/workspace/page-rank/src/main/resources/graph/", 
                              "/Users/rshepherd/Documents/nyu/cloud/workspace/page-rank/src/main/resources/count/"})
           );
    }
}
