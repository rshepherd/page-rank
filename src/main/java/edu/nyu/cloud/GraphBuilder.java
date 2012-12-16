package edu.nyu.cloud;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GraphBuilder
{
    private static final char SEP = '\f';
    
    private static final Pattern TITLE = Pattern.compile("<title>(.*?)</title>", Pattern.DOTALL);
    private static final Pattern LINK = Pattern.compile("\\[\\[(.*?)\\]\\]", Pattern.DOTALL);

    public static class GraphMapper extends Mapper<Object, Text, Text, Text>
    {
        public void map(Object key, Text text, Context context) throws IOException, InterruptedException
        {
            String title = null;
            Matcher matcher = TITLE.matcher(text.toString());
            if (matcher.find())
            {
                title = matcher.group().replaceAll("</?title>", "");
            } else
            {
                System.err.println("No title in document " + text.toString());
                return;
            }

            StringBuffer links = new StringBuffer("1").append(SEP);
            matcher = LINK.matcher(text.toString());
            while (matcher.find())
            {
                String link = matcher.group().replaceAll("[\\[\\]]", "");
                int pipe = link.indexOf("|");
                if (pipe > -1)
                {
                    link = link.substring(0, pipe);
                }
                links.append(link).append(SEP);
            }

            context.write(new Text(title), new Text(links.toString()));
        }
    }

    public static class GraphReducer extends Reducer<Text, Text, Text, Text>
    {

        public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException
        {
            context.write(key, value);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        print("args", args);
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
          System.err.println("Usage: graph <in> <out>");
          System.exit(2);
        }
        Job job = new Job(conf, "graph builder");
        job.setJarByClass(GraphBuilder.class);
        job.setMapperClass(GraphMapper.class);
        job.setReducerClass(GraphReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    private static void print(String name, String[] args) 
    {
        System.out.println(name);
        for(String s : args)
            System.out.print(s+" ");
        System.out.println();
    }
    
}
