package stream2AddDocID;

import matchingClusterNode.MatchingClusterNodeWritable;
import cluster.ClusterFile;
import cluster.ClusterWritable;
import cluster.NodeWritable;
import io.github.htools.collection.ArrayMap;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import kbaeval.TopicFile;
import kbaeval.TopicWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import sentence.SentenceWritable;
import io.github.htools.collection.HashMapList;

/**
 * Add the collection's documentID and sentenceNumber of nodes in query matching
 * clusters.
 * @author jeroen
 */
public class ClusterDocidReducer extends Reducer<IntWritable, SentenceWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(ClusterDocidReducer.class);
    Conf conf;
    ArrayList<ClusterWritable> clusters;
    HashMapList<Long, NodeWritable> sentenceId2Node;

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        clusters = new ArrayList();
        sentenceId2Node = new HashMapList();
        readQueryMatchingClusters(context, clusters, sentenceId2Node);
    }

    @Override
    public void reduce(IntWritable key, Iterable<SentenceWritable> values, Context context) throws IOException, InterruptedException {
        for (SentenceWritable value : values) {
            ArrayList<NodeWritable> records = sentenceId2Node.get(value.sentenceID);
            for (NodeWritable record : records) {
               record.docid = value.getDocumentID();
               record.sentenceNumber = value.sentenceNumber;
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        ClusterFile clusterFile = getOutFile(context);
        clusterFile.openWrite();
        for (ClusterWritable cluster : clusters)
            cluster.write(clusterFile);
        clusterFile.closeWrite();
    }

    public ClusterFile getOutFile(Context context) throws IOException {
        HDFSPath dir = conf.getHDFSPath("output");
        Datafile outfile = dir.getFile(getInputClusterFile(context).getName());
        return new ClusterFile(outfile);
    }
    
    /**
     * @return ClusterFile based on reducer nr and the list of cluster files to process
     */
    public Datafile getInputClusterFile(Context context) throws IOException {
        ArrayList<Datafile> inFiles = ClusterDocidJob.getQueryMatchingClusterFiles(conf);
        return inFiles.get(ContextTools.getTaskID(context));
    }
    
    public void readQueryMatchingClusters(Context context, 
            ArrayList<ClusterWritable> clusters, 
            HashMapList<Long, NodeWritable> sentenceId2Node) throws IOException {
        Datafile datafile = getInputClusterFile(context);
        datafile.setBufferSize(100000000);
        ClusterFile clusterFile = new ClusterFile(datafile);
      
        for (ClusterWritable cluster : clusterFile) {
            clusters.add(cluster);
            for (NodeWritable title : cluster.nodes) {
                sentenceId2Node.add(title.sentenceID, title);
            }
        }
    }
}
