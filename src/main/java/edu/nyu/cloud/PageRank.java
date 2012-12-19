package edu.nyu.cloud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends PageRankTool
{
    
    public static void main(String[] args) throws Exception 
    {
        if (args.length != 3) {
            System.err.println("usage: input-path output-path iterations") ;
            System.exit(-1);
        }
        
        System.exit (
            ToolRunner.run(new Configuration(), new PageRank(), args)
        );
    }
    
    public int run(String[] args) throws Exception
    {
        Util.print("PageRank.run", args);
        
        String inputPath = args[0];
        String outputPath = args[1];
        
        String prevRankPath = outputPath + "/rank/prev";
        String currRankPath = outputPath + "/rank/curr";
        String resultPath = outputPath + "/results";
        
        runTool(new GraphBuilder(), new String[] { inputPath, prevRankPath } );
        runTool(new Ranker(), new String[] { prevRankPath, currRankPath } );
        runTool(new Sorter(), new String[] { currRankPath, resultPath } );
        
        return 0;
    }

    // TODO
    // delete(new Path(outputPath)); // Wipe previous run?
    // move(out, in); // move output of last iter to input  
    
}
