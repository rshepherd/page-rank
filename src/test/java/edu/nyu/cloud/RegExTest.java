package edu.nyu.cloud;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class RegExTest
{
    private final static String text = "<page> <title>Carolus Linnaeus</title>  <text> [[File:Carolus Linnaeus.jpg|right|thumb|A pictur o Carolus Linnaeus]] '''Carl Linnaeus''', kent as as '''Carl von Linné''' aifter his [[nobility|ennoblement]] an aw";

    @Test
    public void test()
    {
        String str = '\t' + "kjasdf " + '\t';
        System.out.println("|"+str.replaceAll("\\W", "")+"|");
        System.exit(0);
        
        
        Pattern pattern = Pattern.compile("<title>(.*?)</title>", Pattern.DOTALL);
        Matcher matcher = pattern.matcher(text);
        if (matcher.find()) { 
           String title = matcher.group();
           System.out.println(title.replaceAll("</?title>", "")); 
        }
        
        pattern = Pattern.compile("\\[\\[(.*?)\\]\\]", Pattern.DOTALL);
        matcher = pattern.matcher(text);
        while (matcher.find()) {
          String link = matcher.group();
          int pipe = link.indexOf("|");
          if(pipe > -1) {
            link = link.substring(0, pipe);
          }
          System.out.println(link.replaceAll("[\\[\\]]", "")); 
        }
        
        System.out.println("true="+("234.23432".matches("((-|\\+)?[0-9]+(\\.[0-9]+)?)+")));
        System.out.println("true="+("23423432".matches("((-|\\+)?[0-9]+(\\.[0-9]+)?)+")));
        System.out.println("false="+("234x23432".matches("((-|\\+)?[0-9]+(\\.[0-9]+)?)+")));
        System.out.println("false="+("".matches("((-|\\+)?[0-9]+(\\.[0-9]+)?)+")));
    }

}
