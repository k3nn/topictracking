package stream4ClusterSentencesPurge;

import io.github.k3nn.Cluster;
import io.github.k3nn.ClusteringGraph;
import io.github.k3nn.impl.NodeSentence;
import cluster.ClusterFile;
import cluster.ClusterWritable;
import cluster.NodeWritable;
import io.github.k3nn.impl.ClusteringGraphPurgable;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import sentence.SentenceWritable;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.hadoop.io.LongBoolWritable;
import io.github.htools.lib.Profiler;
import java.util.HashSet;

/**
 *
 * @author jeroen
 */
public class ClusterSentencesReducer extends Reducer<LongBoolWritable, SentenceWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(ClusterSentencesReducer.class);
    protected Conf conf;
    protected DefaultTokenizer tokenizer = ClusteringGraph.getUnstemmedTokenizer();
    protected ClusterFile clusterFile;
    protected SentenceClusteringGraph<NodeSentence> clusteringGraph;

    enum Counter {

        candidate,
        noncandidate
    }

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        Datafile datafile = conf.getHDFSFile("output");
        clusterFile = new ClusterFile(datafile);
        clusteringGraph = getClusteringGraph();
        clusterFile.openWrite();
    }
    
    public SentenceClusteringGraph<NodeSentence> getClusteringGraph()  throws IOException {
        return new SentenceClusteringGraph(clusterFile);
    }

    @Override
    public void reduce(LongBoolWritable key, Iterable<SentenceWritable> sentences, Context context) throws IOException, InterruptedException {
        for (SentenceWritable sentence : sentences) {
            boolean isCandidate = key.getValue2();
            addSentence(sentence, isCandidate);
            if (key.getValue2()) {
                context.getCounter(Counter.candidate).increment(1);
            } else {
                context.getCounter(Counter.noncandidate).increment(1);
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        clusteringGraph.clusterFile.closeWrite();
        Profiler.reportProfile();
    }

    public void addSentence(SentenceWritable sentenceWritable, boolean isCandidate) {
        HashSet<String> terms = new HashSet(tokenizer.tokenize(sentenceWritable.content));
        if (terms.size() > 1) {
            NodeSentence sentence = new NodeSentence(sentenceWritable.sentenceID,
                    sentenceWritable.domain, sentenceWritable.content, terms,
                    sentenceWritable.creationtime, sentenceWritable.getUUID(),
                    sentenceWritable.sentenceNumber);
            clusteringGraph.addNode(sentence, terms, isCandidate);
        }
    }

    /**
     * extends ClusteringGraph by writing the clusters of candidate sentences
     * that are clustered immediately when added to the ClusterFile.
     */
    public static class SentenceClusteringGraph<N extends NodeSentence> extends ClusteringGraphPurgable<N> {

        public ClusterFile clusterFile;

        public SentenceClusteringGraph(ClusterFile clusterFile) {
            super();
            this.clusterFile = clusterFile;
        }

        /**
         * write clustered candidate sentence to the clusterFile
         */
        @Override
        public void clusteredCandidateSentence(NodeSentence candidateSentence) {
            
            //if (!candidateSentence.getCluster().testCore()) {
            if (candidateSentence.getCluster().misplaced().size() > 0) {
                log.info("%s", candidateSentence.getCluster().evalall());
                log.fatal("failed %d", candidateSentence.getID());
            }
            Cluster<NodeSentence> cluster = candidateSentence.getCluster();
            ClusterWritable clusterWritable = new ClusterWritable();
            clusterWritable.clusterid = cluster.getID();
            for (NodeSentence sentence : cluster.getNodes()) {
                if (sentence != candidateSentence) {
                    clusterWritable.nodes.add(toUrlWritable(sentence));
                }
            }
            clusterWritable.nodes.add(toUrlWritable(candidateSentence));
            clusterWritable.write(clusterFile);
        }

        /**
         * @param sentence
         * @return a NodeWritable that can be used as a member of a
         * ClusterWritable and containing the sentence data.
         */
        public NodeWritable toUrlWritable(NodeSentence sentence) {
            NodeWritable nodeWritable = new NodeWritable();
            nodeWritable.creationtime = sentence.getCreationTime();
            nodeWritable.docid = sentence.getDocumentID();
            nodeWritable.domain = sentence.getDomain();
            nodeWritable.nnid = sentence.getNearestNeighborIds();
            nodeWritable.nnscore = sentence.getNearestNeighborScores();
            nodeWritable.sentenceNumber = sentence.sentence;
            nodeWritable.content = sentence.getContent();
            nodeWritable.sentenceID = sentence.getID();
            return nodeWritable;
        }
    }
}
