package kba4TitleDeduplication;

import sentence.SentenceOutputFormat;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.io.IntLongWritable;
import io.github.htools.hadoop.Job;
import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import sentence.SentenceInputFormat;
import sentence.SentenceWritable;

/**
 * Remove titles, if the exact same title appeared on the same domain within a week.
 * This is a two-stage process. In this first stage, the titles are simply hashed
 * based on domain and contents, and then deduplicated in the reducer. In the
 * second stage they are sorted back to chronological order.
 * 
 * input: folder with SentenceFile per day containing titles
 * output: folder with SentenceFile per day containing deduplicated titles
 * @author jeroen
 */
public class TitleDeduplicationJob {
   public static final Log log = new Log( TitleDeduplicationJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, ParseException {
        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(4096);
        conf.setReduceSpeculativeExecution(false);
        
        String input = conf.get("input");        
        Job job = new Job(conf, input, conf.get("output"));
        
        // 1000 hash buckets to partition the sentences to
        job.setNumReduceTasks(1000);
        job.setInputFormatClass(SentenceInputFormat.class);
        SentenceInputFormat.addDirs(job, input);
        
        job.setMapperClass(TitleDeduplicationMap.class);
        job.setMapOutputKeyClass(IntLongWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);
        
        job.setGroupingComparatorClass(IntWritable.Comparator.class);
        job.setSortComparatorClass(IntLongWritable.SortComparator.class);
        job.setPartitionerClass(IntLongWritable.Partitioner.class);
        job.setReducerClass(TitleDeduplicationReducer.class);
        
        Path out = new Path(conf.get("output"));
        job.setOutputFormatClass(SentenceOutputFormat.class);
        SentenceOutputFormat.setOutputPath(job, out);
        new HDFSPath(conf, out).trash();
        
        job.waitForCompletion(true);
    }
}
