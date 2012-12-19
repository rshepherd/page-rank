package edu.nyu.cloud;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Util
{
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
    
    public static void print(String source, String[] args) 
    {
        System.out.println("args from " + source);
        for(String s : args)
            System.out.println("\t" + s);
    }
    
    public int countLines(FileSystem fs, String pathName) throws IOException 
    {    
        Path file = new Path(pathName + File.separator + "part-r-00000");
        InputStream is = new BufferedInputStream(fs.open(file));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n')
                        ++count;
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }

}
