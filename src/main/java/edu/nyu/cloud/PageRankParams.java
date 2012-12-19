package edu.nyu.cloud;

public interface PageRankParams
{
    static final char DELIMITER = '\t';

    static final double DAMPENING = 0.85;

    static final double CONVERGENCE_CHECK_INTERVAL = 10;

    static final String INIT_PAGE_RANK = "1";
    
    static final String GRAPH_PHASE_DIR = "graph";
    
    static final String RANK_PHASE_DIR = "rank";
    
    static final String CONVERGENCE_PHASE_DIR = "converge";
    
    static final String DANGLING_PHASE_DIR = "dangle";
    
    static final String SORT_PHASE_DIR = "sort";
}
