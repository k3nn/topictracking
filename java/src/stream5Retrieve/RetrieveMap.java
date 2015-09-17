package stream5Retrieve;

import io.github.k3nn.query.Query;
import io.github.k3nn.retriever.TopKRetriever;
import io.github.k3nn.Node;
import io.github.k3nn.impl.NodeSentence;
import matchingClusterNode.MatchingClusterNodeWritable;
import cluster.ClusterWritable;
import cluster.NodeWritable;
import io.github.k3nn.Cluster;
import io.github.k3nn.Edge;
import io.github.k3nn.ClusteringGraph;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.fcollection.FHashSet;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.hadoop.mapreduce.Mapper;
import stream5Retrieve.RetrieveJob.Setting;
import io.github.htools.hadoop.ContextTools;

/**
 * Stream the clusters formed in a per topic clustering graph over all sentences
 * in the news articles of query-matching-title-clusters. The core sentences are
 * used to construct a relevance model tuned to recently seen salient information
 * and the candidate sentences (i.e. clustered in a query matching cluster at arrival)
 * are qualified for emission to the summary.
 * 
 * @author jeroen
 */
public class RetrieveMap extends Mapper<Setting, ClusterWritable, Setting, MatchingClusterNodeWritable> {

    public static final Log log = new Log(RetrieveMap.class);
    static DefaultTokenizer tokenizer = ClusteringGraph.getUnstemmedTokenizer();
    Context context;
    Retriever<NodeSentence> retriever = null;
    // list of clusters that were formed by adding sentences from the current document
    ArrayList<Cluster<NodeSentence>> currentDocClusters;
    // the current docid, used to collect all candidates from the same document
    String currentDocId = "";
    int line = 0;

    @Override
    public void setup(Context context) throws IOException {
        ContextTools.getInputPath(context);
        this.context = context;
        retriever = null;
        currentDocId = "";
    }

    @Override
    public void map(Setting settings, ClusterWritable clusterWritable, Context context) throws IOException, InterruptedException {
//        log.info("%d %d", line++, clusterWritable.clusterid);
        if (retriever == null) { // initialize retriever
            retriever = getRetriever(context, settings);
        }
        // reconstruct the cluster
        Cluster<NodeSentence> cluster = createCluster(clusterWritable);
        
        // collect all clusters formed from the same document and submit these
        // as a batch. Although not used by the baseline, this allows to first update
        // the relevance model with all supporting sentences and then qualify all
        // candidate sentences.
        NodeSentence candidateSentence = cluster.getNodes().get(cluster.size() - 1);
//        log.info("candidate %d %d %s %s", 
//                candidateSentence.getID(), 
//                candidateSentence.getCreationTime(),
//                candidateSentence.getDocumentID(),
//                candidateSentence.getContent());
        if (retriever.notExceedsMaximumLength(candidateSentence.getTerms())) {
            if (!currentDocId.equals(candidateSentence.getDocumentID())) {
                if (currentDocClusters != null && currentDocClusters.size() > 0) {
                    cleanup(context);
                }
                currentDocClusters = new ArrayList();
                currentDocId = candidateSentence.getDocumentID();
            }
            currentDocClusters.add(cluster);
        } else {
            log.info("failed length");
        }
    }

    public Retriever getRetriever(Context context, Setting settings) throws IOException {
        return new Retriever(context, settings);
    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        
        // stream all the cluster through the retriever to emit sentences that
        // qualify through its emit() method to the summary.
        if (currentDocClusters != null && currentDocClusters.size() > 0) {
            log.info("cleanup %d", currentDocClusters.size());
            for (Cluster<NodeSentence> cluster : currentDocClusters) {
                retriever.qualify(cluster);
            }
        }
    }
    
    /**
     * @param clusterWritable
     * @return reconstructed nearest neighbor cluster from stored record
     */
    public static Cluster<NodeSentence> createCluster(ClusterWritable clusterWritable) {
        // we are not really clustering, but using a ClusteringGraph to reconstruct
        // a cluster read from file
        ClusteringGraph<NodeSentence> clusteringGraph = new ClusteringGraph();
        
        // create the sentences
        for (NodeWritable sentenceWritable : clusterWritable.nodes) {
            HashSet<String> terms = new HashSet(tokenizer.tokenize(sentenceWritable.content));
            NodeSentence sentence = new NodeSentence(sentenceWritable.sentenceID, 
                    sentenceWritable.domain, 
                    sentenceWritable.content, 
                    terms, 
                    sentenceWritable.creationtime, 
                    sentenceWritable.getUUID(), 
                    sentenceWritable.sentenceNumber);
            
            clusteringGraph.getNodes().put(sentence.getID(), sentence);
        }
        
        // create the cluster and link the nearest neighbors
        ArrayList<NodeSentence> clusterNodes = new ArrayList();
        for (NodeWritable r : clusterWritable.nodes) {
            NodeSentence node = clusteringGraph.getNode(r.sentenceID);
            ArrayList<Long> nn = r.getNearestNeighborIds();
            ArrayList<Double> score = r.getNearestNeighborScores();
            for (int i = 0; i < nn.size(); i++) {
                Node u = clusteringGraph.getNode(nn.get(i));
                Edge e = new Edge(u, score.get(i));
                node.add(e);
            }
            clusterNodes.add(node);
        }
        
        // find the 2-k degenerate core that for the cluster's base nodes
        Cluster<NodeSentence> cluster = clusteringGraph.createClusterFromSet(clusterWritable.clusterid, clusterNodes);
        
        return cluster;
    }

    public static class Retriever<N extends Node> extends TopKRetriever<N> {
        Setting setting;
        Context context;
        MatchingClusterNodeWritable record = new MatchingClusterNodeWritable();
       
        protected Retriever(Context context, Setting setting) throws IOException {
            this.context = context;
            this.setting = setting;
            this.windowRelevanceModelHours = setting.hours;
            this.maxSentenceLengthWords = setting.length;
            this.minInformationGain = setting.gainratio;
            this.minRankObtained = setting.topk;
            this.init(setting.topicid, setting.topicstart, setting.topicend, Query.create(tokenizer, setting.query));
//            log.info("topic %d %d %d %s", setting.topicid, setting.topicstart, setting.topicend, setting.query);
//            log.info("settings %f %f %d %d", setting.gainratio, setting.hours, setting.length, setting.topk);
        }

        @Override
        public void emit(int topic, Node sentences, String content) throws IOException, InterruptedException {
            record.clusterID = topic;
            record.creationTime = sentences.getCreationTime();
            record.domain = sentences.getDomain();
            record.nnid = sentences.getNearestNeighborIds();
            record.nnscore = sentences.getNearestNeighborScores();
            record.content = content;
            record.sentenceID = sentences.getID();
            record.documentID = ((NodeSentence) sentences).getDocumentID();
            record.sentenceNumber = ((NodeSentence) sentences).sentence;
//            log.info("out %f %f %d %d %d %d %s", setting.gainratio, setting.hours, setting.length, setting.topk, 
//                    setting.topicid, record.creationTime, record.documentID);
            context.write(setting, record);
        }
    }
}
