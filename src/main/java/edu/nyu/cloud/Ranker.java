package edu.nyu.cloud;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Ranker
{
    // Input record format: inlink \t pagerank \t outlink1 \t outlink2 \t ...
    // Emit key-value pairs: key=url value=rank -or- key=url value=outlinks
    public static class RankMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            
            // Emit each outlink and its rank
            StringTokenizer st = new StringTokenizer(line, PageRank.DELIMITER+"");
            Text inlink = new Text(st.nextToken());
            Text outlinkRank = rank(st.nextToken(), st.countTokens());
            while (st.hasMoreTokens())
            {
                Text outlink = new Text(st.nextToken()); 
                context.write(outlink, outlinkRank);
            }
            
            // Emit the inlink and all of its outlinks
            int outlinksIndex = PageRank.nthIndexOf(line, PageRank.DELIMITER, 2) + 1;
            context.write(inlink, new Text(line.substring(outlinksIndex)));
        }

        private Text rank(String parentPageRank, int numOutlinks)
        {
            Double rank = Double.valueOf(parentPageRank) / numOutlinks;
            return new Text( rank.toString() );
        }
    }

    // Emit: key=url value=pagerank \t outlink1 \t outlink2 \t ...
    public static class RankReducer extends Reducer<Text, Text, Text, Text>
    {

        public void reduce(Text key, Iterator<Text> values, Context context)
                throws IOException, InterruptedException
        {
            double rank = 0.0f;
            StringBuilder outlinks = new StringBuilder();

            while (values.hasNext())
            {
                String v = values.next().toString();
                if (PageRank.isNumeric(v))
                {
                    rank += Double.valueOf(v);
                } else
                {
                    outlinks.append(PageRank.DELIMITER).append(v);
                }
            }

            context.write(key, new Text(String.valueOf(rank) + outlinks.toString()));
        }
    }

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException
    {
        PageRank.printArray(args);
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3)
        {
            System.err.println("Usage: rank <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "page rank");
        job.setJarByClass(Ranker.class);
        job.setMapperClass(RankMapper.class);
        job.setReducerClass(RankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
