

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
    User : Ganga
    Date : 05/15/2020

    The following MapReduce Java program was written as part of course COEN242 - BigData Assignment2

    input : 32 gb text file.
    output: top 100 words with length > 6.


    Algorithm:
    - Read the input file by using Mapper function. Here the Mapper tasks can be set by providing input split using Configuration.
            -the mapper function reads each line of text file and filters out all the unnecessary characters from the string.
            -StringTokenizer is used to break line into words and add to map.
            - here the mapper function further filters out words which are less than length 6.

    - the Reducer task, then collects all data from mapper and sums up the values.
    - after this cleanup function is called which in turn calls the sortMapByValues function.
            - this sortMapByValues function, sorts the Map according to the values in descending order.
            - then, the Collections.sort() method is used to sort the entries in map in descending order.
            - After this, linkedhashmap is used to add all entries and to maintain the order of insertion.
            - Returns the sorted map to calling funciton.

     - Then the cleanup function iterates the sorted map and writes only top100 words.

 */

public class WordCount2 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            //filtering the unwanted special characters
            String line = value.toString().replaceAll("[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']", " ").trim();
            String[] wordList = line.split(" ");


            //check length of each word in list.
            //if greater than 6, only then context
            for (String s : wordList)
            {
                int length = s.length();
                if (length > 6)
                {
                    word.set(s);
                    context.write(word,one);
                }
            }
        }
    }
    //the reducer gets word and adds to hashmap
    //if key already present, then increments the value, else add key and set value to 1

    //code reference: https://github.com/andreaiacono/MapReduce/blob/master/src/main/java/samples/topn/TopN.java

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {


        private Map<Text,IntWritable> wordMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            wordMap.put(new Text(key),new IntWritable((sum)));
        }

        @Override
        protected  void cleanup(Context context) throws IOException,InterruptedException
        {

            Map<Text, IntWritable> sortedMap = sortMapByValues(wordMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 100) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }

        /**
         * sorts the map by values. Taken from:
         * http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
         */
        public static <K extends Comparable, V extends Comparable> Map<K, V> sortMapByValues(Map<K, V> map) {
            List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

            Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

                @Override
                public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //LinkedHashMap will keep the keys in the order they are inserted
            //which is currently sorted on natural ordering
            Map<K, V> sortedMap = new LinkedHashMap<K, V>();

            for (Map.Entry<K, V> entry : entries) {
                sortedMap.put(entry.getKey(), entry.getValue());
            }

            return sortedMap;
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        //setting up maximum and min split, file size in bytes. Here the 32gb file is split into 512MB splits.
        //the code is tested with this setup and also by commenting this out to test for default mappers.
        conf.set("mapred.min.split.size","536870912");
        conf.set("mapred.max.split.size","536870912");


        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCount2.class);
        job.setMapperClass(WordCount2.TokenizerMapper.class);

        //This is combiner class to improve performance.
        job.setCombinerClass(WordCount2.IntSumReducer.class);

        job.setReducerClass(WordCount2.IntSumReducer.class);
        //to set reducer tasks
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
