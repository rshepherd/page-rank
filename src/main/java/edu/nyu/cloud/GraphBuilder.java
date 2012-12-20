package edu.nyu.cloud;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GraphBuilder extends PageRankTool 
{
    
    public int run(String[] args) throws Exception
    {
        printArgs("GraphBuilder.run", args);
        
        Job job = new Job(getConf(), "Page Rank Graph Builder");
        
        // Config i/o
        Path input = new Path(args[0]);  // wikipedia data 
        FileInputFormat.addInputPath(job, input);
        Path output = new Path(args[1]); // 'previous' rank data
        FileOutputFormat.setOutputPath(job, output);
        
        // Config job
        job.setJarByClass(GraphBuilder.class);
        job.setMapperClass(GraphMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static class GraphMapper extends Mapper<Object, Text, Text, Text>
    {
        private static final Log LOG = LogFactory.getLog(GraphMapper.class);
        
        private static final Pattern TITLE_PATTERN  = Pattern.compile("<title>(.*?)</title>", Pattern.DOTALL);
        private static final Pattern LINK_PATTERN   = Pattern.compile("\\[\\[(.*?)\\]\\]", Pattern.DOTALL);
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String pageBody = value.toString();
            
            // Extract the pagerank 'url'; the canonical name of a wikipedia page.
            // Page names are found between <title> tags.
            String pageName; 
            Matcher matcher = TITLE_PATTERN.matcher(pageBody);
            if (matcher.find())
            {
                pageName = matcher.group().replaceAll("</?title>", "").trim();
            } 
            else
            {
                LOG.warn("No title in document " + pageBody);
                return;
            }

            // Extract the outbound links, filter dupes and self-references.
            // Links are in the format [[ page name | display text ]].
            StringBuilder links = new StringBuilder(PageRankParams.INIT_RANK+"");
            Set<String> dupeFilter = new HashSet<String>();
            matcher = LINK_PATTERN.matcher(pageBody);
            while (matcher.find())
            {
                String link = matcher.group().replaceAll("[\\[\\]]", "").trim();
                int pipeIndex = link.indexOf("|");
                if (pipeIndex > -1)
                {
                    link = link.substring(0, pipeIndex);
                }
                // Ignore duplicates or links itself
                if (!pageName.equals(link) && dupeFilter.add(link))
                {
                    links.append(PageRankParams.DELIM).append(link);
                }
            }
            
            // Output record format: link \t pagerank \t outlink1 \t outlink2 \t ...
            context.write(new Text(pageName), new Text(links.toString()));
        }
    }
   
}
