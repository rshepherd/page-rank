package edu.nyu.cloud;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ranker extends PageRankTool 
{
    
    public int run(String[] args) throws Exception
    {
        Util.print("Ranker.run", args);
        
        Job job = new Job(getConf(), "Page Rank Ranking Iteration");
        
        // Config file system
        Path input = new Path(args[0] + "/part-r-00000");  
        Path output = new Path(args[1]); 
        FileOutputFormat.setOutputPath(job, output);
        FileInputFormat.addInputPath(job, input);
        
        // Config job
        job.setJarByClass(Ranker.class);
        job.setMapperClass(RankMapper.class);
        job.setReducerClass(RankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static class RankMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        static final Log LOG = LogFactory.getLog(RankMapper.class);
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            
            // Emit each outlink and its rank
            StringTokenizer st = new StringTokenizer(line, PageRankParams.DELIMITER+"");
            Text inlink = new Text(st.nextToken());
            Text outlinkRank = rank(st.nextToken(), st.countTokens());
            while (st.hasMoreTokens())
            {
                Text outlink = new Text(st.nextToken()); 
                context.write(outlink, outlinkRank);
            }
            
            // Emit the inlink and all of its outlinks
            int outlinksIndex = Util.nthIndexOf(line, PageRankParams.DELIMITER, 2) + 1;
            context.write(inlink, new Text(line.substring(outlinksIndex)));
        }

        private Text rank(String parentPageRank, int numOutlinks)
        {
            if (numOutlinks == 0)
            {
                return null;
            }
            Double rank = Double.valueOf(parentPageRank) / numOutlinks;
            return new Text(rank.toString());
        }
    }

    // Emit: key=url value=pagerank \t outlink1 \t outlink2 \t ...
    public static class RankReducer extends Reducer<Text, Text, Text, Text>
    {
        static final Log LOG = LogFactory.getLog(RankReducer.class);
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            double rank = 0.0;
            StringBuilder outlinks = new StringBuilder();

            Iterator<Text> i = values.iterator();
            while (i.hasNext())
            {
                String v = i.next().toString();
                if (Util.isNumeric(v))
                {
                    rank += Double.valueOf(v);
                } else
                {
                    outlinks.append(PageRankParams.DELIMITER).append(v);
                }
            }
            
            rank = (1 - PageRankParams.DAMPENING) + (PageRankParams.DAMPENING * rank);
            
            // Input record format: inlink \t pagerank \t outlink1 \t outlink2 \t ...
            context.write(key, new Text(String.valueOf(rank) + outlinks.toString()));
        }
    }

}
