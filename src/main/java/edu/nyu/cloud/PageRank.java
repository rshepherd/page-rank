package edu.nyu.cloud;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends PageRankTool
{
    public static final char   DELIM = '\t';
    public static final double DAMP_FACTOR = 0.85;
    public static final double INIT_RANK = 0.15;
    public static final int    CONVERGENCE_INTERVAL = 3;
    public static final double CONVERGENCE_TOLERANCE = 0.001;
    public static final String OUTPUT_FILENAME = "/part-r-00000";
    
    @Override
    public int run(String[] args) throws Exception
    {
        // Parse arguments
        String inputPath = args[0];
        outputPath = args[1];
        int iterations = Integer.parseInt(args[2]);
        
        // Define output paths
        String prevRankPath  = outputPath + "/rank/prev";
        String currRankPath  = outputPath + "/rank/curr";
        String danglerPath   = outputPath + "/rank/dang";
        String counterPath   = outputPath + "/rank/count";
        String convergePath  = outputPath + "/rank/conv";
        String resultsPath   = outputPath + "/results";
        
        // Initialize the graph
        runPhase(new GraphBuilder(), new String[] { inputPath, prevRankPath } );
        runPhase(new Ranker(), new String[] { prevRankPath, currRankPath } );
        
        // Find out how many pages total there are
        runPhase(new Counter(), new String[] { currRankPath, counterPath } );
        String linkCount = getLinkCount(counterPath);
        
        // Execute iterations
        for (int i = 1; i < iterations; ++i)
        {
            // The output of the last iteration is now the 'previous' output
            mv(currRankPath, prevRankPath);
            
            // Get total rank of all danglers to be divided among all other nodes
            String danglerRankSum = getDanglerRankSum(prevRankPath, danglerPath);
            
            // Run the ranker again.
            runPhase(new Ranker(), new String[] { prevRankPath, currRankPath, linkCount, danglerRankSum });

            // Maybe check for convergence
            if (i % CONVERGENCE_INTERVAL == 0)
            {
                runPhase(new ConvergenceDetector(), new String[] { currRankPath, convergePath });
                double rankDifferential = getRankDifferential(convergePath);
                rm(convergePath);
                if (rankDifferential <= (CONVERGENCE_TOLERANCE * Double.valueOf(linkCount)))
                {
                    System.out.println("Convergence reached at iteration " + (i+1));
                    break;
                } 
            }
            
            System.out.println("Completed at iteration " + (i+1));
        }
        
        // Finalize output
        runPhase(new Sorter(), new String[] { currRankPath, resultsPath } );
        
        // Clean-up
        rm(outputPath + "/rank");

        return 0;
    }

    private String getDanglerRankSum(String prevRankPath, String danglerPath) throws Exception, IOException
    {
        runPhase(new DanglerAccumulator(), new String[] { prevRankPath, danglerPath });
        String danglersRankSum = getDanglersRankSum(danglerPath);
        rm(danglerPath);
        return danglersRankSum;
    }
    
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

}
