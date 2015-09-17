package stream4ClusterSentencesPurge;

import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import sentence.SentenceInputFormat;
import sentence.SentenceWritable;
import io.github.htools.hadoop.io.LongBoolWritable;
import org.apache.hadoop.io.LongWritable;
import static stream4ClusterSentences.ClusterSentencesJob.getRelevantDocs;
import static stream4ClusterSentences.ClusterSentencesJob.setInputFiles;

public class ClusterSentencesJob {

    private static final Log log = new Log(ClusterSentencesJob.class);
    static Conf conf;

    public static Job setup(String args[]) throws IOException {
        conf = new Conf(args, "-i input -o output -d source");
        conf.setReduceSpeculativeExecution(false);
        conf.setTaskTimeout(100 * 6 * 60 * 60);
        conf.setReduceMemoryMB(8192);
        conf.setMaxReduceAttempts(1);
        getRelevantDocs(conf);

        Job job = new Job(conf, conf.get("input"), conf.get("output"), conf.get("source"));
                //job.getConfiguration().setInt("mapreduce.task.timeout", 1800000);
        
        job.setInputFormatClass(SentenceInputFormat.class);
        setInputFiles(job);

        job.setNumReduceTasks(1);
        job.setMapperClass(ClusterSentencesMap.class);
        job.setReducerClass(ClusterSentencesReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(LongBoolWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);
        job.setSortComparatorClass(LongWritable.Comparator.class);

        return job;
    }
    
    public static void main(String[] args) throws Exception {
        Job job = setup(args);
        job.waitForCompletion(true);
    }
}
