package edu.nyu.cloud;

import java.io.IOException;

public class PageRank
{
    public static final char DELIMITER = '\t';
    
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException
    {
        if (args.length > 0 && args[0].equals("graph"))
        {
            GraphBuilder.main(args);
        } else if (args.length > 0 && args[0].equals("rank"))
        {
            Ranker.main(args);
        } else if (args.length > 0 && args[0].equals("sort"))
        {
            Sorter.main(args);
        } else
        {
            System.out.println("Specify a job.");
        }
    }

    public static boolean isNumeric(String s)
    {
        return s.matches("((-|\\+)?[0-9]+(\\.[0-9]+)?)+");
    }
    
    public static int nthIndexOf(String str, char c, int n) {
        n--;
        int pos = str.indexOf(c, 0);
        while (n-- > 0 && pos != -1)
            pos = str.indexOf(c, pos+1);
        return pos;
    }
    
    public static void printArray(String[] args) 
    {
        for(String s : args)
            System.out.print(s+" ");
        System.out.println();
    }

}
