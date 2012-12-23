package edu.nyu.cloud;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Extension of a Configured Tool impl. with convenience methods useful
 * in various stages of the page rank job flow
 */
public abstract class PageRankTool extends Configured implements Tool 
{
    protected final Log LOG = LogFactory.getLog(this.getClass());

    protected String outputPath;
    
    public abstract int run(String[] args) throws Exception;
    
    protected int runPhase(Tool tool, String[] args) throws Exception
    {
        return ToolRunner.run( 
            getConf(), 
            tool,
            args
        );
    }
    
    protected FileSystem getFileSystem() throws Exception 
    {
        URI uri = new URI(outputPath);
        Configuration c = getConf();
        FileSystem fs = FileSystem.get(uri, c);
        return fs;
    }
    
    protected boolean rm(String path) throws Exception 
    {
        Path p = new Path(path);
        FileSystem fs = getFileSystem();
        return fs.delete(p, true);
    }
    
    protected void mv(String from, String to) throws Exception {
        Path src = new Path(from);
        Path dst = new Path(to);
        FileSystem fs = getFileSystem();
        fs.delete(dst, true) ;
        fs.rename(src,  dst) ;
    }

    protected String getDanglersRankSum(String pathName) throws Exception
    {
        FileSystem fs = getFileSystem();
        String fileName = pathName + PageRank.OUTPUT_FILENAME;
        String danglerTotalRank = readOneToken(fs,fileName);
        return danglerTotalRank != null ? danglerTotalRank : "0";
    }
    
    protected Double getRankDifferential(String pathName) throws Exception
    {
        FileSystem fs = getFileSystem();
        String fileName = pathName + PageRank.OUTPUT_FILENAME;
        String rankDifferential = readOneToken(fs,fileName);
        if(rankDifferential == null) {
            throw new RuntimeException("Unable to retrieve rank differential.");
        }
        return Double.valueOf(rankDifferential);
    }

    protected String getLinkCount(String pathName) throws Exception, URISyntaxException
    {
        FileSystem fs = getFileSystem();
        String fileName = pathName + PageRank.OUTPUT_FILENAME;
        String linkCount = readOneToken(fs,fileName);
        return linkCount != null ? linkCount : "0";
    }
    
    protected String readOneToken(FileSystem fs, String fileName)
    {
        try
        {
            Path file = new Path(fileName);
            InputStreamReader stream = new InputStreamReader(fs.open(file));
            BufferedReader in = new BufferedReader(stream);
            StringTokenizer st = new StringTokenizer(in.readLine(), PageRank.DELIM + "");
            String token = st.nextToken();
            in.close();
            return token;
        } catch (Exception e)
        {
            return null;
        }
    }
    
    public static void printArgs(String source, String[] args) 
    {
        System.out.println(source + " arguments:");
        for(String s : args)
            System.out.println("\t" + s);
    }

}
