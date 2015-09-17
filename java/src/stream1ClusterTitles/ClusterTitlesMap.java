package stream1ClusterTitles;

import clusterNode.ClusterNodeFile;
import clusterNode.ClusterNodeWritable;
import io.github.k3nn.Cluster;
import io.github.k3nn.Edge;
import io.github.k3nn.ClusteringGraph;
import io.github.k3nn.Node;
import io.github.k3nn.impl.NodeCount;
import cluster.ClusterWritable;
import cluster.NodeWritable;
import io.github.htools.collection.ArrayMap;
import io.github.htools.collection.ArrayMap3;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.lib.DateTools;
import io.github.htools.lib.Log;
import io.github.htools.type.Tuple2;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import sentence.SentenceFile;
import sentence.SentenceWritable;
import io.github.htools.collection.HashMapList;
import io.github.htools.fcollection.FHashSet;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.hadoop.io.IntIntWritable;
import io.github.htools.hadoop.io.IntLongIntWritable;
import static kba6PreClusterTitles.ClusterTitlesMap.findNextClusterID;

/**
 * Clusters the titles of one single day, starting with the clustering results
 * at the end of yesterday,
 *
 * @author jeroen
 */
public class ClusterTitlesMap extends Mapper<LongWritable, SentenceWritable, IntLongIntWritable, ClusterWritable> {

    public static final Log log = new Log(ClusterTitlesMap.class);
    // tokenizes on non-alphanumeric characters, lowercase, stop words removed, no stemming
    DefaultTokenizer tokenizer = ClusteringGraph.getUnstemmedTokenizer();
    // Map<TopicID, List<SentenceID that contain all query terms>>
    HashMap<Long, ArrayList<Integer>> sentence2TopicReducer;
    // Map of topicID and query matching cluster
    ArrayMap<IntLongIntWritable, ClusterWritable> outRecords = new ArrayMap();
    Conf conf;
    // clustering graph used to cluster the sentences
    ClusteringGraph<NodeCount> titleClusteringGraph;
    // folder of the midgnight snapshots for the title clustering
    HDFSPath titleClusterSnapshots;
    Date today;
    int sequence = 0;

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        titleClusterSnapshots = conf.getHDFSPath("clusters");
        sentence2TopicReducer = ClusterTitlesJob.getTopicMatchingSentences(conf);
        titleClusteringGraph = new ClusteringGraph();

        // extract today from input file, e.g. filename must be yyyy-mm-dd
        String todayString = ContextTools.getInputPath(context).getName();
        try {
            today = DateTools.FORMAT.Y_M_D.toDate(todayString);
        } catch (ParseException ex) {
            log.fatalexception(ex, "illegal date %s", todayString);
        }
        readClusters();
    }

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) {
        HashSet<String> terms = new HashSet(tokenizer.tokenize(value.content));
        try {
            // only use titles with more than one word
            if (terms.size() > 1) {
                // add title to clustering graph
                if (titleClusteringGraph.getNodes().containsKey(value.sentenceID)) {
                    log.info("existing %d %s", value.sentenceID, value.creationtime, value.content);
                }

                NodeCount title = new NodeCount(value.sentenceID, value.domain, value.creationtime, terms);
                titleClusteringGraph.addNode(title, terms);

                // if clustered and it is a query matching cluster, prepare to emit
                if (title.isClustered()) {
                    this.addCandidateNode(title);
                }
            }
        } catch (Exception ex) {
            log.fatalexception(ex, "");
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        titleClusteringGraph = null; // release memory

        // read titles for the clusters to emit
        HashMap<Long, String> id2title = getTitlesYesterday();
        id2title.putAll(getTitlesToday());

        // add titles to nodes, and emit
        for (Map.Entry<IntLongIntWritable, ClusterWritable> entry : outRecords) {
            ClusterWritable clusterWritable = entry.getValue();
            for (NodeWritable title : clusterWritable.nodes) {
                title.content = id2title.get(title.sentenceID);
            }

            // send to reducer
            context.write(entry.getKey(), clusterWritable);
        }
    }

    /**
     * if candidateNode is in a query matching cluster, prepare to emit
     *
     * @param candidateTitle
     */
    private void addCandidateNode(Node candidateTitle) {
        Cluster<NodeCount> cluster = candidateTitle.getCluster();

        //list of reducers to send this cluster to
        HashSet<Integer> reducers = new HashSet();

        for (Node title : cluster.getNodes()) {
            Collection<Integer> topicReducers = sentence2TopicReducer.get(title.getID());
            if (topicReducers != null) {
                reducers.addAll(topicReducers);
            }
        }
        for (int reducer : reducers) {
            addQueryMatchingClusterToReducer(reducer, candidateTitle, cluster);
        }
    }

    /**
     * Prepare a query matching cluster for emission to the reducer of the topic
     *
     * @param reducer
     * @param candidateTitle
     * @param cluster
     */
    private void addQueryMatchingClusterToReducer(int reducer, Node candidateTitle, Cluster<NodeCount> cluster) {
        ClusterWritable clusterWritable = new ClusterWritable();
        clusterWritable.clusterid = cluster.getID();
        for (Node title : cluster.getNodes()) {
            if (title != candidateTitle) {
                clusterWritable.nodes.add(toNodeWritable(title));
            }
        }
        // sort the cluster members chronologically, making sure the candidate 
        // (last arriving) title is in last position.
        Collections.sort(clusterWritable.nodes, Sorter.instance);
        clusterWritable.nodes.add(toNodeWritable(candidateTitle));

        IntLongIntWritable key = new IntLongIntWritable(reducer, candidateTitle.getCreationTime(), sequence++);
        outRecords.add(key, clusterWritable);
    }

    private NodeWritable toNodeWritable(Node title) {
        NodeWritable nodeWritable = new NodeWritable();
        nodeWritable.creationtime = title.getCreationTime();
        nodeWritable.domain = title.getDomain();
        nodeWritable.nnid = title.getNearestNeighborIds();
        nodeWritable.nnscore = title.getNearestNeighborScores();
        nodeWritable.sentenceID = title.getID();
        return nodeWritable;
    }

    // sort on creation time
    private static class Sorter implements Comparator<NodeWritable> {

        static Sorter instance = new Sorter();

        @Override
        public int compare(NodeWritable n1, NodeWritable n2) {
            return (int) (n1.creationtime - n2.creationtime);
        }
    }

    /**
     * read cluster snapshot at the end of yesterday, as the starting point to
     * cluster todays titles in a simulated online setting
     */
    public void readClusters() throws IOException, IOException, IOException, IOException, IOException {
        Date previousdate = DateTools.daysBefore(today, 1);
        Datafile datafile = titleClusterSnapshots.getFile(DateTools.FORMAT.Y_M_D.format(previousdate));
        if (datafile.exists() && datafile.getLength() > 0) {
            // map to temporarily store the cluster assigments
            HashMapList<Integer, NodeCount> clusterNodes = new HashMapList();
            
            // map to cash nearest neigbor information, which is added after all
            // nodes are read
            ArrayMap3<Long, ArrayList<Long>, ArrayList<Double>> nearestNeighbors = new ArrayMap3();
            ClusterNodeFile clusterNodeFile = new ClusterNodeFile(datafile);
            for (ClusterNodeWritable clusterNode : clusterNodeFile) {
                NodeCount title = (NodeCount) titleClusteringGraph.getNode(clusterNode.sentenceID);
                if (title == null) { // only if not exists
                    //log.info("%d %d %d %s", clusterNode.sentenceID, clusterNode.domain, clusterNode.creationTime, clusterNode.content);
                    title = getTitle(clusterNode.sentenceID, clusterNode.domain, clusterNode.creationTime, clusterNode.content);
                    if (title != null) { // can be null if there is only one unique non stop word in title
                        nearestNeighbors.add(title.getID(), clusterNode.getNearestNeighborIds(), clusterNode.getNearestNeighborScores());

                        // if clusterID >= 0, assign it to its cluster
                        if (clusterNode.clusterID >= 0) {
                            clusterNodes.add(clusterNode.clusterID, title);
                        }
                    }
                }
            }

            // add the nearest neighbor links
            for (Map.Entry<Long, Tuple2<ArrayList<Long>, ArrayList<Double>>> entry : nearestNeighbors) {
                NodeCount title = titleClusteringGraph.getNode(entry.getKey());
                for (int i = 0; i < entry.getValue().key.size(); i++) {
                    long nearestNeighborId = entry.getValue().key.get(i);
                    NodeCount nearestNeighbor = titleClusteringGraph.getNode(nearestNeighborId);
                    double nearestNeighborScore = entry.getValue().value.get(i);
                    Edge edge = new Edge(nearestNeighbor, nearestNeighborScore);
                    title.add(edge);
                }
            }

            // construct the clusters for which there exsists a valid
            // 2-degenerate core (should always be the case), and assign the
            // other nodes (majority votes) as members.
            for (Map.Entry<Integer, ArrayList<NodeCount>> clusterid2Nodes : clusterNodes.entrySet()) {
                int clusterid = clusterid2Nodes.getKey();
                ArrayList<NodeCount> nodes = clusterid2Nodes.getValue();
                titleClusteringGraph.createClusterFromSet(clusterid, nodes);
            }

        } else {
            // when a snapshot for yesterday does not exist (i.e. all news articles
            // expired the T=3 days constraint), we look back further to find the
            // last file before the gap to find the next id to use for a new cluster
            titleClusteringGraph.setStartClusterID(findNextClusterID(titleClusterSnapshots, today));
        }
    }

    public HashMap<Long, String> getTitlesYesterday() {
        HashMap<Long, String> sentenceId2Title = new HashMap();
        Date previousdate = DateTools.daysBefore(today, 1);

        Datafile datafile = titleClusterSnapshots.getFile(DateTools.FORMAT.Y_M_D.format(previousdate));
        if (datafile.exists()) {
            ClusterNodeFile clusterNodeFile = new ClusterNodeFile(datafile);
            for (ClusterNodeWritable clusterNodeWritable : clusterNodeFile) {
                sentenceId2Title.put(clusterNodeWritable.sentenceID, clusterNodeWritable.content);
            }
        }
        return sentenceId2Title;
    }

    public HashMap<Long, String> getTitlesToday() {
        HashMap<Long, String> sentenceId2Title = new HashMap();
        Datafile datafile = conf.getHDFSPath("input").getFile(DateTools.FORMAT.Y_M_D.format(today));
        if (datafile.exists()) {
            SentenceFile sentenceFile = new SentenceFile(datafile);
            for (SentenceWritable sentence : sentenceFile) {
                sentenceId2Title.put(sentence.sentenceID, sentence.content);
            }
        }
        return sentenceId2Title;
    }

    /**
     * @param sentenceid
     * @param domain
     * @param creationtime
     * @param title
     * @return created Node from yesterday's snapshot, added to the stream pool
     * and inverted index
     */
    public NodeCount getTitle(long sentenceid, int domain, long creationtime, String title) {
        NodeCount node = (NodeCount) titleClusteringGraph.getNode(sentenceid);
        if (node == null) {
            HashSet<String> features = new HashSet(tokenizer.tokenize(title));
            if (features.size() > 1) {
                node = new NodeCount(sentenceid, domain, creationtime, features);
                titleClusteringGraph.getNodes().put(node.getID(), node);
                titleClusteringGraph.nodeStore.add(node, features);
            }
        }
        return node;
    }
}
