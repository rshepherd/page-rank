package edu.nyu.cloud;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;

public class PageRankerTest
{

    @Test
    public void test() throws ClassNotFoundException, IOException, InterruptedException
    {
        
        //inlink \t pagerank \t outlink1 \t outlink2 \t ...
        for(int i = 0 ; i < 10 ; ++i) 
        {
            map("!Xaaaa      1       Botswana        Namibie L");
        }
    }
    
    private void map(String line) throws IOException, InterruptedException
    {
        StringTokenizer st = new StringTokenizer(line, PageRank.DELIMITER+"");
        Text inlink = new Text(st.nextToken());
        Text outlinkRank = rank(st.nextToken(), st.countTokens());
        while (st.hasMoreTokens())
        {
            System.out.println(st.nextToken() + PageRank.DELIMITER + outlinkRank);
        }
        
        int outlinksIndex = PageRank.nthIndexOf(line, PageRank.DELIMITER, 2) + 1;
        System.out.println(inlink + (PageRank.DELIMITER+"")  + line.substring(outlinksIndex));
    }
    

    private Text rank(String parentPageRank, int numOutlinks)
    {
        Double rank = Double.valueOf(parentPageRank) / numOutlinks;
        return new Text( rank.toString() );
    }

}
