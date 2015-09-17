package kba5TitleSortTimestamp;

import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.io.DayPartitioner;
import io.github.htools.hadoop.Job;
import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import sentence.SentenceInputFormat;
import sentence.SentenceWritable;

/**
 * Sort the collection of titles on Timestamp, for online processing.
 * input: folder of SentenceFile, unsorted
 * output: folder of SentenceFile, grouped in a YYYY-MM-DD file and within each file on creationtime.
 * startdate: YYYY-MM-DD, the first date in the corpus
 * enddate: YYYY-MM-DD, the last date in the corpus
 * remark: forgot to use the original sentenceID to keep titles with the same creationtime 
 * in the original order, which means slight differences are to be expected in reproduced results.
 * @author jeroen
 */
public class SortJob {
   public static final Log log = new Log( SortJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, ParseException {
        Conf conf = new Conf(args, "-i input -o output -s startdate -e enddate");
        conf.setMapMemoryMB(4096);
        conf.setReduceSpeculativeExecution(false);

        DayPartitioner.setTime(conf, conf.get("startdate"), conf.get("enddate"));
        int reducers = DayPartitioner.getNumberOfReducers(conf);
               
        Job job = new Job(conf, 
                conf.get("input"), 
                conf.get("output"), 
                conf.get("startdate"), 
                conf.get("enddate"), 
                reducers);
        
        job.setNumReduceTasks(reducers);
        job.setInputFormatClass(SentenceInputFormat.class);
        SentenceInputFormat.addDirs(job, conf.get("input"));
        
        job.setMapperClass(SortMap.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);
        
        job.setSortComparatorClass(LongWritable.Comparator.class);
        job.setPartitionerClass(DayPartitioner.class);
        job.setReducerClass(SortReducer.class);
        
        job.setOutputFormatClass(NullOutputFormat.class);
        conf.getHDFSPath("output").trash();
        
        job.waitForCompletion(true);
    }
   
}
