package edu.nyu.cloud;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

// TODO: cleanup. delete previous run

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
        printArgs("PageRank.run", args);
        
        // Parse arguments
        String inputPath = args[0];
        String outputPath = args[1];
        int iterations = Integer.parseInt(args[2]);
        
        // Define output paths
        String prevRankPath = outputPath + "/rank/prev";
        String currRankPath = outputPath + "/rank/curr";
        String danglersPath = outputPath + "/rank/dang";
        String resultsPath  = outputPath + "/results";
        
        // Initialize the job
        runTool(new GraphBuilder(), new String[] { inputPath, prevRankPath } );
        runTool(new Ranker(), new String[] { prevRankPath, currRankPath } );
        String linkCount = getLinkCount(currRankPath);
        move(new Path(currRankPath), new Path(prevRankPath)) ;
        
        // Execute iterations
        for (int i = 1; i < iterations; ++i)
        {
            runTool(new DanglerRankAccumulator(), new String[] { prevRankPath, danglersPath });
            String danglerDistro = getDanglerDistribution(danglersPath);
            runTool(new Ranker(), new String[] { prevRankPath, currRankPath, linkCount, danglerDistro });
            move(new Path(currRankPath), new Path(prevRankPath));
            delete(new Path(danglersPath));
        }
        
        // Finalize output
        runTool(new Sorter(), new String[] { prevRankPath, resultsPath } );
        
        return 0;
    }

    private String getDanglerDistribution(String danglersPath) throws IOException
    {
        String danglerTotalRank = FileUtils.readOneToken(getFileSystem(),
                danglersPath + PageRankParams.OUTPUT_FILENAME);

        return danglerTotalRank != null ? danglerTotalRank : "0";
    }

    private String getLinkCount(String prevRankPath) throws IOException
    {
        // TODO - Use mapreduce for this
        return String.valueOf(FileUtils.countLines(getFileSystem(), prevRankPath
                + PageRankParams.OUTPUT_FILENAME));
    }

}
