import java.util.*;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapred.*; 

public class mapReduceQ
{ 
   public static class QMapper 
        extends MapReduceBase implements Mapper<LongWritable ,Text,Text,IntWritable>         
   { 
      public void map(LongWritable key, Text value, 
                      OutputCollector<Text, IntWritable> output,   
                      Reporter reporter) throws IOException 
      { 
         String line = value.toString(); 
         
         StringTokenizer s = new StringTokenizer(line,"\t"); 
          IntWritable one = new IntWritable(1);
         //To consume student id
         String studentId = s.nextToken(); 
         String schoolGrade = null; 
          String schoolGrade_category = null; 
         
         while(s.hasMoreTokens())
            {
               schoolGrade=s.nextToken();
               int schoolGrade_int = Integer.parseInt(schoolGrade); 
               if(schoolGrade_int<3){
                schoolGrade_category="C";
               };
               if(schoolGrade_int>=3 && schoolGrade_int<5){
                schoolGrade_category="B";
               }
               if(schoolGrade_int>=5 ){
                schoolGrade_category="A";
               };
                output.collect(new Text(schoolGrade_category), one);
            } 
            
          
      } 
   } 
   
   
 
   public static class QReduce extends MapReduceBase implements 
   Reducer< Text, IntWritable, Text, IntWritable > 
   {  
   

      public void reduce( Text key, Iterator <IntWritable> values, 
         OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
         { 
            int suma = 0;
            while (values.hasNext()) 
            { 
               suma += values.next().get(); 
            }
            output.collect(key, new IntWritable(suma));
         } 
   }  
   
   

   public static void main(String args[])throws Exception 
   { 
      JobConf conf = new JobConf(mapReduceQ.class);
      
      conf.setJobName("count_schoolGrade_category"); 
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(IntWritable.class); 
      conf.setMapperClass(QMapper.class); 
      conf.setCombinerClass(QReduce.class); 
      conf.setReducerClass(QReduce.class); 
      conf.setInputFormat(TextInputFormat.class); 
      conf.setOutputFormat(TextOutputFormat.class); 
      
      FileInputFormat.setInputPaths(conf, new Path(args[0])); 
      FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
      
      JobClient.runJob(conf); 
   } 
} 