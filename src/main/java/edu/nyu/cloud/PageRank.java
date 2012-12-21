package edu.nyu.cloud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends PageRankTool
{
    public static final char   DELIM = '\t';
    public static final double DAMP_FACTOR = 0.85;
    public static final double INIT_RANK = 0.15;
    public static final int    CONVERGENCE_INTERVAL = 3;
    public static final double CONVERGENCE_TOLERANCE = 0.00001;
    public static final String OUTPUT_FILENAME = "/part-r-00000";
    
    @Override
    public int run(String[] args) throws Exception
    {
        // Parse arguments
        String inputPath = args[0];
        String outputPath = args[1];
        int iterations = Integer.parseInt(args[2]);
        
        // Define output paths
        String prevRankPath  = outputPath + "/rank/prev";
        String currRankPath  = outputPath + "/rank/curr";
        String danglerPath   = outputPath + "/rank/dang";
        String convergePath  = outputPath + "/rank/conv";
        String resultsPath   = outputPath + "/results";
        
        // Initialize job
        runPhase(new GraphBuilder(), new String[] { inputPath, prevRankPath } );
        runPhase(new Ranker(), new String[] { prevRankPath, currRankPath } );
        String linkCount = getLinkCount(currRankPath);
        mv(currRankPath, prevRankPath);
        
        // Execute iterations
        for (int i = 1; i < iterations; ++i)
        {
            System.out.println("Iteration " + i);
            runPhase(new DanglerAccumulator(), new String[] { prevRankPath, danglerPath });
            runPhase(new Ranker(), new String[] { prevRankPath, currRankPath, linkCount, getDanglersRankSum(danglerPath) });
            mv(currRankPath, prevRankPath);
            rm(danglerPath);

            if (i % CONVERGENCE_INTERVAL == 0)
            {
                runPhase(new ConvergenceDetector(), new String[] { prevRankPath, convergePath });
                if (getRankDifferential(convergePath) <= CONVERGENCE_TOLERANCE)
                {
                    System.out.println("Convergence reached at " + i + "th iteration.");
                    rm(convergePath);
                    break;
                }
            }
        }
        
        // Finalize output
        runPhase(new Sorter(), new String[] { prevRankPath, resultsPath } );
        rm(prevRankPath);

        return 0;
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
