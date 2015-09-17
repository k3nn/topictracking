package stream4ClusterSentences;

import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.InputFormat;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.DateTools;
import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import stream3DocStream.DocumentStreamFile;
import stream3DocStream.DocumentStreamWritable;
import sentence.SentenceInputFormat;
import sentence.SentenceWritable;
import io.github.htools.collection.HashMap3;
import io.github.htools.hadoop.io.IntBoolWritable;
import io.github.htools.hadoop.io.LongBoolWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class ClusterSentencesJob {

    private static final Log log = new Log(ClusterSentencesJob.class);

    public static Job setup(String args[]) throws IOException {
        Conf conf = new Conf(args, "-i input -o output -d source");
        conf.setReduceSpeculativeExecution(false);
        conf.setReduceMemoryMB(8192);
        //getRelevantDocs(conf);

        Job job = new Job(conf, conf.get("input"), conf.get("output"), conf.get("source"));
        
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

    /**
     * @param conf
     * @return map of documents that were seen in query matching clusters, 
     * consisting of the TREC UUID, creation time and whether the document is a 
     * candidate (was clustered in a query matching cluster upon arrival).
     */
    public static HashMap3<String, Long, Boolean> getRelevantDocs(Configuration conf) {
        HashMap3<String, Long, Boolean> documents = new HashMap3();
        Datafile df = new Datafile(conf, conf.get(("input")));
        DocumentStreamFile docfile = new DocumentStreamFile(df);
        for (DocumentStreamWritable d : docfile) {
            documents.put(d.docid, d.creationtime, d.isCandidate);
        }
        return documents;
    }
    
    public static void setInputFiles(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        HashSet<String> dates = new HashSet();

        for (String docid : getRelevantDocs(conf).keySet()) {
            String part[] = docid.split("-");
            if (part.length == 2) {
                long creationtime = Long.parseLong(part[0]);
                Date creationdate = DateTools.epochToDate(creationtime);
                dates.add(DateTools.FORMAT.Y_M_D.format(creationdate));
            }
        }
        
        HDFSPath inpath = new HDFSPath(conf, conf.get("source"));
        for (String date : dates) {
            InputFormat.addDirs(job, inpath.getFilename(date));
        }
    }
    
    public static void main(String[] args) throws Exception {
        Job job = setup(args);
        job.waitForCompletion(true);
    }
}
