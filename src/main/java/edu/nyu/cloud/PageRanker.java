package edu.nyu.cloud;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRanker
{

    public static class PageRankMapper extends Mapper<Text, Text, Text, Text>
    {
        // Input key=url value=[rank, outlinks]
        public void map(Text key, Text val, Context context)
                throws IOException, InterruptedException
        {
            String value = val.toString();
            String[] rankAndLinks = value.split(Driver.SEP + "");
            Integer numOutLinks = rankAndLinks.length - 1;
            Float rank = Float.valueOf(rankAndLinks[0]) / numOutLinks;
            for (int i = 1; i < numOutLinks; ++i)
            {
                // Emit key=outlink value=rank
                context.write(new Text(rankAndLinks[i]), new Text(rank.toString()));
            }

            // Emit key=url value=outlinks
            context.write(key, new Text(value.substring(value.indexOf(Driver.SEP) + 1)));
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text>
    {
        // Input key=outlink value=rank -or- key=url value=outlinks
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> context, Reporter reporter)
                throws IOException, InterruptedException
        {
            float rank = 0.0f;
            StringBuilder outlinks = new StringBuilder();

            while (values.hasNext())
            {
                String v = values.next().toString();
                if (Driver.isNumeric(v))
                {
                    rank += Float.valueOf(v);
                } else
                {
                    outlinks.append(v).append(Driver.SEP);
                }
            }

            // Emit key=url value=[rank, outlinks]
            context.collect(key, new Text(String.valueOf(rank) + outlinks.toString()));
        }
    }

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 3)
        {
            System.err.println("Usage: rank <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "page rank");
        job.setJarByClass(PageRanker.class);
        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
