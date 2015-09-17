package kba6PreClusterTitles;

import io.github.htools.lib.Log;
import io.github.htools.hadoop.ConfSetting;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import sentence.SentenceInputFormat;

/**
 * Cluster the titles in an online setting (in chronological order). 
 * Since the clustering of t-1 is needed for the clustering at time t, this 
 * cannot be done in parallel, and therefore should be done one day at-a-time. 
 * However, fast reproduction is achieved by first clustering the titles in
 * chronological order and saving a snapshot at the end of each day. This allows
 * to reproduce the title clustering by processing each day in parallel starting
 * with the snapshot at the end of the previous day.
 * input: folder with YYYY-MM-DD daily SentenceFile with sorted and deduplicated titles
 * of news articles in the KBA corpus
 * output: 
 * 
 * @author jeroen
 */
public class ClusterTitlesJob {

    private static final Log log = new Log(ClusterTitlesJob.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(1800000);
        // result files are written directly, therefore no speculative execution
        conf.setMapSpeculativeExecution(false);
        conf.setMaxMapAttempts(1);

        Job job = new Job(conf, conf.get("input"), conf.get("output"));

        job.setInputFormatClass(SentenceInputFormat.class);
        SentenceInputFormat.addDirs(job, conf.get("input"));
        SentenceInputFormat.setNonSplitable(job);

        job.setNumReduceTasks(0);
        job.setMapperClass(ClusterTitlesMap.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.waitForCompletion(true);
    }
}
