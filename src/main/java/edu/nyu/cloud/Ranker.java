package edu.nyu.cloud;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class Ranker extends PageRankTool 
{
    
    public int run(String[] args) throws Exception
    {
        printArgs("Ranker.run", args);
        
        Job job = new Job(getConf(), "Page Rank Ranking Iteration");
        
        // Config i/o
        Path input = new Path(args[0] + PageRank.OUTPUT_FILENAME);  
        FileInputFormat.addInputPath(job, input);
        Path output = new Path(args[1]); 
        FileOutputFormat.setOutputPath(job, output);
        
        // Config job
        job.setJarByClass(Ranker.class);
        job.setMapperClass(RankMapper.class);
        job.setReducerClass(RankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Optional params
        if(args.length == 4){ 
            JobConf conf = (JobConf) job.getConfiguration();
            conf.set("link.count", args[2]);
            conf.set("dangler.distro", args[3]);  
        }
        
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class RankMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        static final Log LOG = LogFactory.getLog(RankMapper.class);
        
        @Override @SuppressWarnings("unused")
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer st = new StringTokenizer(value.toString(), PageRank.DELIM+"");
            Text pageName = new Text(st.nextToken());
            Double pageRank = Double.parseDouble(st.nextToken());
            String oldPageRank = st.nextToken(); 
            
            // Emit each outlink and its new rank
            StringBuilder outlinks = new StringBuilder();
            Text outlinkRank = sliceRank(pageRank, st.countTokens());
            while (st.hasMoreTokens())
            {
                Text outlink = new Text(st.nextToken()); 
                context.write(outlink, outlinkRank);
                outlinks.append(outlink).append(PageRank.DELIM);
            }
            
            // Emit the page name and all of its outlinks
            context.write(pageName, new Text(outlinks.toString()));
            
            // Emit the page name and its current page rank
            context.write(pageName, new Text("pr="+pageRank.toString()));
        }

        private Text sliceRank(Double parentPageRank, int numOutlinks)
        {
            if (numOutlinks == 0)
            {
                return null;
            }
            Double rank = parentPageRank / numOutlinks;
            return new Text(rank.toString());
        }
        
    }

    // Emit: key=url value=pagerank \t old-pagerank \t outlink1 \t outlink2 \t ...
    public static class RankReducer extends Reducer<Text, Text, Text, Text>
    {
        static final Log LOG = LogFactory.getLog(RankReducer.class);
        
        private Double danglerContribution = 0.0;
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            double newRank = 0.0;
            StringBuilder oldRank = new StringBuilder();
            StringBuilder outlinks = new StringBuilder();

            for (Text value : values)
            {
                String v = value.toString();
                if (isNumeric(v))
                {
                    newRank += Double.valueOf(v);
                } 
                else if (v.startsWith("pr="))
                {
                    v = v.replaceAll("pr=", "");
                    oldRank.append(PageRank.DELIM).append(v);
                } 
                else
                {
                    outlinks.append(PageRank.DELIM).append(v);
                }
            }
            
            // Special case for 1st iteration. Graph builder output
            // does not have entries for danglers and therefore no old rank
            if(oldRank.length() == 0) {
                oldRank.append(PageRank.DELIM).append(String.valueOf(newRank));
            }
            
            newRank = 1 - PageRank.DAMP_FACTOR + (PageRank.DAMP_FACTOR * newRank);
            newRank += danglerContribution;
            
            // Output record format: inlink \t pagerank \t old-pagerank \t outlink1 \t outlink2 \t ...
            context.write(key, new Text(String.valueOf(newRank) + oldRank.toString() + outlinks.toString()));
        }
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException
        {
            try
            {
                int linkCount = Integer.valueOf (
                   context.getConfiguration().get("link.count")
                );
                double danglerDistro = Double.valueOf (
                    context.getConfiguration().get("dangler.distro")
                );
                danglerContribution = danglerDistro / linkCount;
            } catch (NumberFormatException e)
            {
                LOG.warn("Unable to parse dangler and link count params.");
            }
        }
        
        private boolean isNumeric(String s)
        {
            return s.matches("((-|\\+)?[0-9]+(\\.[0-9]+)?)+");
        }
    }
    
    public static void main(String[] args) throws Exception {
        System.exit (
           ToolRunner.run (
               new Configuration(), 
               new Ranker(), 
               new String[] { "/Users/rshepherd/Documents/nyu/cloud/workspace/page-rank/src/main/resources/rank/", 
                              "/Users/rshepherd/Documents/nyu/cloud/workspace/page-rank/src/main/resources/rank2/"})
           );
    }

}
