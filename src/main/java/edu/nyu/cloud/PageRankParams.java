package edu.nyu.cloud;

public interface PageRankParams
{
    static final char DELIM = '\t';

    static final double DAMP_FACTOR = 0.85;

    static final double INIT_RANK = 1.0 - DAMP_FACTOR;
    
    static final double CONVERGENCE_CHECK_INTERVAL = 10;
    
    static final String OUTPUT_FILENAME = "/part-r-00000";
}
