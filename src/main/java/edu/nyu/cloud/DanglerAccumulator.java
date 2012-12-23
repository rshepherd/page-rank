package edu.nyu.cloud;

import java.io.IOException;
import java.util.StringTokenizer;

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

// Accumulate ranks of all dangling nodes for distribution across 
// the rest of the nodes in the graph by the Ranker.

public class DanglerAccumulator extends PageRankTool
{

    @Override
    public int run(String[] args) throws Exception
    {
        printArgs("DanglerRankAccumulator.run", args);
        
        Job job = new Job(getConf(), "Dangler Accumulator");
        
        // Config i/o
        Path input = new Path(args[0]);// + PageRank.OUTPUT_FILENAME);  
        FileInputFormat.addInputPath(job, input);
        Path output = new Path(args[1]); 
        FileOutputFormat.setOutputPath(job, output);
        
        // Config job
        job.setJarByClass(DanglerAccumulator.class);
        job.setMapperClass(DanglerMapper.class);
        job.setReducerClass(DanglerReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class DanglerMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        private static final Text DANGLER_KEY = new Text("DANGLER");
        
        @Override @SuppressWarnings("unused")
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            StringTokenizer st = new StringTokenizer(value.toString(), PageRank.DELIM+"");
            String pageName = st.nextToken(); 
            String pageRank = st.nextToken();
            String oldPageRank = st.nextToken();
            if (!st.hasMoreTokens())
            {
                context.write(DANGLER_KEY, new Text(pageRank));
            }
        }
    }

    public static class DanglerReducer extends Reducer<Text, Text, Text, NullWritable>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            Double rank = 0.0;
            for(Text v : values)
            {
                rank += Double.valueOf(v.toString());
            }
            context.write(new Text(rank.toString()), NullWritable.get());
        }
    }
    
    public static void main(String[] args) throws Exception {
        System.exit (
           ToolRunner.run (
               new Configuration(), 
               new DanglerAccumulator(), 
               new String[] { "/Users/rshepherd/Documents/nyu/cloud/workspace/page-rank/src/main/resources/rank2/", 
                              "/Users/rshepherd/Documents/nyu/cloud/workspace/page-rank/src/main/resources/dang/"})
           );
    }

}
