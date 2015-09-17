package stream2AddDocID;

import cluster.ClusterFile;
import cluster.ClusterWritable;
import cluster.NodeWritable;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.io.IntPartitioner;
import io.github.htools.hadoop.Job;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import sentence.SentenceInputFormat;
import sentence.SentenceWritable;
import io.github.htools.collection.HashMapList;

/**
 * Retrieves the docIDs and adds these to the query matching clusters.
 * input: folder with YYYY-MM-DD SentenceFile with the titles for the entire corpus (kba5)
 * querymatchingclusters: folder with ClusterFile per topic, that contains the query matching title
 * clusters that were formed during clustering.
 * topicfile: TREC .xml file that contains the topics, using TopicFile reader
 * output: folder with ClusterFile per topic, to which the documentIDs were added
 * 
 * @author jeroen
 */
public class ClusterDocidJob {

    private static final Log log = new Log(ClusterDocidJob.class);

    public static Job setup(String args[]) throws IOException {
        Conf conf = new Conf(args, "-i input -o output -r querymatchingclusters -t topicfile");
        conf.setReduceSpeculativeExecution(false);
        conf.setTaskTimeout(60000000);
        conf.setReduceMemoryMB(4096);

        setTargetedSentenceIds(conf);

        Job job = new Job(conf, 
                conf.get("input"), 
                conf.get("output"), 
                conf.get("querymatchingclusters"), 
                conf.get("topicfile"));

        job.setInputFormatClass(SentenceInputFormat.class);
        SentenceInputFormat.addDirs(job, conf.get("input"));

        job.setNumReduceTasks(getQueryMatchingClusterFiles(conf).size());
        job.setMapperClass(ClusterDocidMap.class);
        job.setReducerClass(ClusterDocidReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);
        job.setPartitionerClass(IntPartitioner.class);

        conf.getHDFSFile("output").trash();
        return job;
    }

    /**
     * Stores a list of nodeID's for which docid's have to be retrieved
     * in the configuration
     * @param conf
     */
    public static void setTargetedSentenceIds(Conf conf) throws IOException {
        ArrayList<Datafile> queryMatchingClusterFiles = getQueryMatchingClusterFiles(conf);
        for (int topicIndex = 0; topicIndex < queryMatchingClusterFiles.size(); topicIndex++) {
            Datafile queryMatchingClusterDatafile = queryMatchingClusterFiles.get(topicIndex);
            queryMatchingClusterDatafile.setBufferSize(10000000);
            ClusterFile clusterFile = new ClusterFile(queryMatchingClusterDatafile);
            
            HashSet<Long> sentenceIds = new HashSet();
            for (ClusterWritable cluster : clusterFile) {
                for (NodeWritable node : cluster.nodes)
                   sentenceIds.add(node.sentenceID);
            }
            conf.setLongList("resultids." + topicIndex, sentenceIds);
        }
        conf.setInt("topics", queryMatchingClusterFiles.size());
    }
    
    /**
     * @param conf
     * @return a map of reducer ID, list of nodeIDs for which docIDs are retrieved
     */
    public static HashMapList<Long, Integer> getTargetedSentenceIds(Conf conf) {
        HashMapList<Long, Integer> sentenceId2TopicReducer = new HashMapList();
        int topics = conf.getInt("topics", 0);
        for (int topicIndex = 0; topicIndex < topics; topicIndex++) {
            ArrayList<Long> sentenceIds = conf.getLongList("resultids." + topicIndex);
            for (long sentenceId : sentenceIds) {
                sentenceId2TopicReducer.add(sentenceId, topicIndex);
            }
        }
        return sentenceId2TopicReducer;
    }
    
    /**
     * @param conf
     * @return list of result files, for which docIDs must be retrieved
     */
    public static ArrayList<Datafile> getQueryMatchingClusterFiles(Conf conf) throws IOException {
        HDFSPath queryMatchingClusterPath = conf.getHDFSPath("querymatchingclusters");
        return queryMatchingClusterPath.getFiles();
    }

    public static void main(String[] args) throws Exception {
        Job job = setup(args);
        job.waitForCompletion(true);
    }
}
