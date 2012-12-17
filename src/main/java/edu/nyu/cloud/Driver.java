package edu.nyu.cloud;

import java.io.IOException;

public class Driver
{
 
    public static final char SEP = '\f';
    
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException
    {
        GraphBuilder.main(args);
    }
    
    public static void print(String name, String[] args) 
    {
        System.out.println(name);
        for(String s : args)
            System.out.print(s+" ");
        System.out.println();
    }

    public static boolean isNumeric(String s)
    {
        return s.matches("((-|\\+)?[0-9]+(\\.[0-9]+)?)+");
    }

}
