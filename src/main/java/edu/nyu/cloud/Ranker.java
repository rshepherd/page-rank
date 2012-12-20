package edu.nyu.cloud;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ranker extends PageRankTool 
{
    
    public int run(String[] args) throws Exception
    {
        printArgs("Ranker.run", args);
        
        Job job = new Job(getConf(), "Page Rank Ranking Iteration");
        
        // Config i/o
        Path input = new Path(args[0] + PageRankParams.OUTPUT_FILENAME);  
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
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            
            // Emit each outlink and its rank
            StringTokenizer st = new StringTokenizer(line, PageRankParams.DELIM+"");
            Text pageName = new Text(st.nextToken());
            Double pageRank = Double.parseDouble(st.nextToken());
            Text outlinkRank = sliceRank(pageRank, st.countTokens());
            
            StringBuilder outlinks = new StringBuilder();
            while (st.hasMoreTokens())
            {
                Text outlink = new Text(st.nextToken()); 
                context.write(outlink, outlinkRank);
                outlinks.append(PageRankParams.DELIM).append(outlink);
            }
            
            String links = outlinks.toString();
            if (!links.isEmpty())
            {
                links = links.substring(1);
            } 
            
            // Emit the page name and all of its outlinks
            context.write(pageName, new Text(links));
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

    // Emit: key=url value=pagerank \t outlink1 \t outlink2 \t ...
    public static class RankReducer extends Reducer<Text, Text, Text, Text>
    {
        static final Log LOG = LogFactory.getLog(RankReducer.class);
        
        private Double danglerContribution = 0.0;
        
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
                LOG.info("RJS - Unable to parse config params.");
            }
        }
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            double rank = 0.0;
            StringBuilder outlinks = new StringBuilder();

            for (Text value : values)
            {
                String v = value.toString();
                if (isNumeric(v))
                {
                    rank += Double.valueOf(v);
                } else
                {
                    outlinks.append(PageRankParams.DELIM).append(v);
                }
            }
            
            rank = 1 - PageRankParams.DAMP_FACTOR + (PageRankParams.DAMP_FACTOR * rank);
            rank += danglerContribution;
            
            // Output record format: inlink \t pagerank \t outlink1 \t outlink2 \t ...
            context.write(key, new Text(String.valueOf(rank) + outlinks.toString()));
        }
        
        private boolean isNumeric(String s)
        {
            return s.matches("((-|\\+)?[0-9]+(\\.[0-9]+)?)+");
        }
    }

}
