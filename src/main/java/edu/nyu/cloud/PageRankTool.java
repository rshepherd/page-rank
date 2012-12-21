package edu.nyu.cloud;

import java.io.IOException;

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

    public abstract int run(String[] args) throws Exception;
    
    protected int runPhase(Tool tool, String[] args) throws Exception
    {
        return ToolRunner.run( 
            getConf(), 
            tool,
            args
        );
    }
    
    protected boolean rm(String path) throws IOException 
    {
        Path p = new Path(path);
        FileSystem fs = getFileSystem();
        return fs.delete(p, true);
    }
    
    protected void mv(String from, String to) throws IOException {
        Path src = new Path(from);
        Path dst = new Path(to);
        FileSystem fs = getFileSystem();
        fs.delete(dst, true) ;
        fs.rename(src,  dst) ;
    }

    protected FileSystem getFileSystem() throws IOException 
    {
        Configuration c = getConf();
        FileSystem fs = FileSystem.get(c);
        return fs;
    }
    
    protected String getDanglersRankSum(String pathName) throws IOException
    {
        FileSystem fs = getFileSystem();
        String fileName = pathName + PageRank.OUTPUT_FILENAME;
        String danglerTotalRank = FileUtils.readOneToken(fs,fileName);
        return danglerTotalRank != null ? danglerTotalRank : "0";
    }
    
    protected Double getRankDifferential(String pathName) throws IOException
    {
        FileSystem fs = getFileSystem();
        String fileName = pathName + PageRank.OUTPUT_FILENAME;
        String rankDifferential = FileUtils.readOneToken(fs,fileName);
        if(rankDifferential == null) {
            throw new RuntimeException("Unable to retrieve rank differential.");
        }
        return Double.valueOf(rankDifferential);
    }

    protected String getLinkCount(String pathName) throws IOException
    {
        FileSystem fs = getFileSystem();
        String fileName = pathName + PageRank.OUTPUT_FILENAME;
        return String.valueOf(FileUtils.countLines(fs, fileName));
    }
    
    public static void printArgs(String source, String[] args) 
    {
        System.out.println(source + " arguments:");
        for(String s : args)
            System.out.println("\t" + s);
    }

}
