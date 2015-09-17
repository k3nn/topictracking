package stream3DocStream;

import cluster.ClusterInputFormat;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import sentence.SentenceInputFormat;

/**
 * Transforms a stream of query matching clusters to a list of document IDs that
 * is used for the clustering of sentences contained in those documents. In the
 * resulting list of document IDs, the documents remain in the order they were
 * seen in the title clustering, removing document IDs that were seen previously
 * and marking candidate documents, i.e. documents that were clustered in a
 * query matching cluster immediately on arrival, since these are the only
 * documents we allow the select sentences from.
 * input: folder containing a ClusterFile per topic
 * output: folder that 
 * @author Jeroen
 */
public class DocumentStreamJob {

    private static final Log log = new Log(DocumentStreamJob.class);

    public static Job setup(String args[]) throws IOException {
        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(4096);

        Job job = new Job(conf, conf.get("input"), conf.get("output"));
        
        job.setInputFormatClass(ClusterInputFormat.class);
        SentenceInputFormat.addDirs(job, conf.get("input"));

        job.setNumReduceTasks(0);
        job.setMapperClass(DocumentStreamMap.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        DocumentStreamOutputFormat.setSingleOutput(job, conf.getHDFSPath("output"));

        conf.getHDFSPath("output").trash();
        return job;
    }
    
    public static void main(String[] args) throws Exception {
        Job job = setup(args);
        job.waitForCompletion(true);
    }
}
