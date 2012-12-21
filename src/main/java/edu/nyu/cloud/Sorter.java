package edu.nyu.cloud;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class Sorter extends PageRankTool 
{
    @Override
    public int run(String[] args) throws Exception
    {
        printArgs("Sorter.run", args);
        
        Job job = new Job(getConf(), "Page Rank Sorting");
        
        // Config i/o
        Path input = new Path(args[0] + PageRank.OUTPUT_FILENAME);  
        FileInputFormat.addInputPath(job, input);
        Path output = new Path(args[1]); 
        FileOutputFormat.setOutputPath(job, output);
        
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
            StringTokenizer st = new StringTokenizer(value.toString(), PageRank.DELIM+"");
            Text pageName = new Text(st.nextToken());
            double pageRank = Double.parseDouble(st.nextToken());
            context.write(new DoubleWritable(pageRank), pageName);
        }
    }

    private static class DoubleWritableDecreasing extends DoubleWritable.Comparator {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
    
    public static void main(String[] args) throws Exception {
        System.exit (
           ToolRunner.run (
               new Configuration(), 
               new Sorter(), 
               new String[] { "/Users/rshepherd/Documents/nyu/cloud/workspace/page-rank/src/main/resources/rank2/", 
                              "/Users/rshepherd/Documents/nyu/cloud/workspace/page-rank/src/main/resources/sort/"})
           );
    }

}
