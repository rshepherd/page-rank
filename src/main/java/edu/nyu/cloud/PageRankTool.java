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
 * Extension of Configured Tool with convenience methods useful
 * in various stages of our job flow
 */
public abstract class PageRankTool extends Configured implements Tool 
{
    protected final Log LOG = LogFactory.getLog(this.getClass());

    public abstract int run(String[] args) throws Exception;
    
    protected int runTool(Tool tool, String[] args) throws Exception
    {
        return ToolRunner.run( 
            getConf(), 
            tool,
            args
        );
    }
    
    protected boolean create(Path p) throws IOException 
    {
        FileSystem fs = getFileSystem();
        return fs.create(p, true) != null;
    }
    
    protected boolean delete(Path p) throws IOException 
    {
        FileSystem fs = getFileSystem();
        return fs.delete(p, true);
    }
    
    protected void move(Path moveThis, Path toHere) throws IOException 
    {
        FileSystem fs = getFileSystem();
        fs.delete(toHere, true) ;
        fs.rename(moveThis, toHere) ;
    }

    protected FileSystem getFileSystem() throws IOException 
    {
        Configuration c = getConf();
        FileSystem fs = FileSystem.get(c);
        return fs;
    }

}
