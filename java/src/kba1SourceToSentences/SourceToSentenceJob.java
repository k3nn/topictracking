package kba1SourceToSentences;

import sentence.SentenceOutputFormat;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import sentence.SentenceWritable;
import io.github.htools.hadoop.io.IntLongIntWritable;
import kbaReader.InputFormatKBA;
import kbaReader.InputFormatKBAGZ;

/**
 * Reads the KBA Streaming corpus (tested with 2013 edition), and writes the 
 * contents as in SentenceFile format. Since we had problems on our cluster
 * processing all KBA files in one jobs, we split jobs per day, writing the output
 * to a single SentenceFile.
 * @author jeroen
 */
public class SourceToSentenceJob {
   public static final Log log = new Log( SourceToSentenceJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(8192);
        
        // every map reads (part of) an archive file, and sends it to the reducer
        // to store in a chronological archive of sentences
        // a different job is performed per date in the corpus
        Job job = new Job(conf, conf.get("input"), conf.get("output"));
        
        HDFSPath in = conf.getHDFSPath("input");
        // inputformat KBAGZ is for the repacked .gz archive, use KBA for the original .xz archive
        job.setInputFormatClass(InputFormatKBAGZ.class);
        // add all files as input for the job
        InputFormatKBA.addDirs(job, in);
        
        job.setMapperClass(SourceToSentenceMap.class);
        job.setMapOutputKeyClass(IntLongIntWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);
        
        job.setGroupingComparatorClass(IntLongIntWritable.Comparator.class);
        job.setSortComparatorClass(IntLongIntWritable.SortComparator.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(SourceToSentenceReducer.class);
                
        Path out = new Path(conf.get("output"));
        new SentenceOutputFormat(job);
        SentenceOutputFormat.setSingleOutput(job, out);
        
        job.waitForCompletion(false);
    }
   
}
