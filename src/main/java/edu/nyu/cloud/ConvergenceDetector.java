package edu.nyu.cloud;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ConvergenceDetector extends PageRankTool
{

    @Override
    public int run(String[] args) throws Exception
    {
        printArgs("ConvergenceDetector.run", args);
        
        Job job = new Job(getConf(), "Page Rank Convergence Detector");
        
        // Config i/o
        Path input = new Path(args[0] + PageRank.OUTPUT_FILENAME);  
        FileInputFormat.addInputPath(job, input);
        Path output = new Path(args[1]); 
        FileOutputFormat.setOutputPath(job, output);
        
        // Config job
        job.setJarByClass(ConvergenceDetector.class);
        job.setMapperClass(ConvergenceMapper.class);
        job.setReducerClass(ConvergenceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static class ConvergenceMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        static final Log LOG = LogFactory.getLog(ConvergenceMapper.class);
        
        private static final Text CONVERGENCE_KEY = new Text("CONVERGENCE");
        
        @Override @SuppressWarnings("unused")
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer st = new StringTokenizer(value.toString(), PageRank.DELIM+"");
            String pageName = st.nextToken(); 
            Double pageRank = Double.valueOf(st.nextToken());
            Double oldPageRank = Double.valueOf(st.nextToken());
            Double difference = Math.abs(pageRank - oldPageRank);
            context.write(CONVERGENCE_KEY, new Text(difference.toString()));
        }
    }
    
    public static class ConvergenceReducer extends Reducer<Text, Text, Text, NullWritable>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            Double totalDifferece = 0.0;
            for(Text v : values)
            {
                totalDifferece += Double.valueOf(v.toString());
            }
            context.write(new Text(totalDifferece.toString()), NullWritable.get());
        }
    }

}
