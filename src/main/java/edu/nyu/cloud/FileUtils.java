package edu.nyu.cloud;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileUtils
{
    public static int countLines(FileSystem fs, String fileName) throws IOException
    {
        Path file = new Path(fileName);
        InputStream is = new BufferedInputStream(fs.open(file));
        try
        {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1)
            {
                empty = false;
                for (int i = 0; i < readChars; ++i)
                {
                    if (c[i] == '\n')
                        ++count;
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally
        {
            is.close();
        }
    }
    
    public static String readOneToken(FileSystem fs, String fileName)
    {
        try
        {
            Path file = new Path(fileName);
            InputStreamReader stream = new InputStreamReader(fs.open(file));
            BufferedReader in = new BufferedReader(stream);
            StringTokenizer st = new StringTokenizer(in.readLine(), PageRankParams.DELIM + "");
            String token = st.nextToken();
            in.close();
            return token;
        } catch (Exception e)
        {
            return null;
        }
    }

}
