package kba6PreClusterTitles;

import clusterNode.ClusterNodeFile;
import clusterNode.ClusterNodeWritable;
import io.github.k3nn.Cluster;
import io.github.k3nn.Edge;
import io.github.k3nn.Node;
import io.github.k3nn.ClusteringGraph;
import io.github.k3nn.impl.NodeCount;
import io.github.htools.collection.ArrayMap;
import io.github.htools.collection.ArrayMap3;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.lib.DateTools;
import io.github.htools.lib.Log;
import io.github.htools.lib.Profiler;
import io.github.htools.type.Tuple2;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import sentence.SentenceFile;
import sentence.SentenceWritable;
import io.github.htools.collection.HashMapList;
import io.github.htools.fcollection.FHashSet;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;

/**
 * Clusters the titles that arrive in a day, expanding the clustering results of
 * the previous day. The titles arrive in order of timestamp. It assumes that
 * the input filename equals todays date in YYYY-MM-DD format. Based on this,
 * the output path is scanned to read the results at the end of yesterday's
 * clustering.
 * <p/>
 * The output consists of all title nodes, clustered and not clustered, that
 * have not expired the T=3 days window, i.e. unclustered nodes that are less
 * than 4 days old, and all nodes in a cluster that has at least one node that
 * has not expired. Since the similarity between nodes is 0 when their creation
 * times are more than T apart, pruning these nodes will not affect future
 * clustering results, which we verified to be correct for the KBA corpus.
 *
 * @author jeroen
 */
public class ClusterTitlesMap extends Mapper<LongWritable, SentenceWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(ClusterTitlesMap.class);
    Conf conf;
    // tokenizes words by breaking on non-alphanumeric characters, lowercasing, 
    // removing stop words, but does not stem.
    DefaultTokenizer tokenizer = ClusteringGraph.getUnstemmedTokenizer();
    ClusterNodeWritable clusterwritable = new ClusterNodeWritable();
    // a 3NN clustering stream
    ClusteringGraph<NodeCount> clusteringGraph;
    Date today;
    long expireTime;

    public enum LABEL {

        CORRECT, TOOSHORT
    };

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        clusteringGraph = new ClusteringGraph();
        today = today(context);
        expireTime = DateTools.daysBefore(today, 3).getTime() / 1000;
        readClusters();
    }

    @Override
    public void map(LongWritable key, SentenceWritable title, Context context) {
        ArrayList<String> features = tokenizer.tokenize(title.content);
        try {
            // only use titles with more than 1 word
            if (features.size() > 1) {
                context.getCounter(LABEL.CORRECT).increment(1);

                // use unique non stop words in title
                HashSet<String> uniq = new HashSet(features);
                // create node with the title's features
                NodeCount node = new NodeCount(title.sentenceID, title.domain, title.creationtime, uniq);
                // add the node to the clustering graph, assign nearest neighbors
                // and update the clustering.
                clusteringGraph.addNode(node, uniq);
            } else {
                context.getCounter(LABEL.TOOSHORT).increment(1);
            }
        } catch (Exception ex) {
            log.fatalexception(ex, "");
        }
    }

    /**
     * write the non expired clusters and unclustered nodes at the end of the
     * day
     */
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        // map<nodeID, Node> of all non expired nodes
        HashMap<Long, Node> outTitlesMap = new HashMap();
        for (Cluster<NodeCount> cluster : clusteringGraph.getClusters()) {
            if (!cluster.getNodes().containsAll(cluster.getCore())) {
                log.info("FAIL %s", cluster.evalall());
                log.crash();
            }
            addNonExpiredClusters(outTitlesMap, cluster);
        }
        addNonExpiredUnclusteredNodes(outTitlesMap);
        clusteringGraph = null; // free memory

        // map<clusterID, Node> that is sorted, id=-1 is used when not clustered
        ArrayMap<Integer, Node> outTitlesList = new ArrayMap();
        for (Node title : outTitlesMap.values()) {
            Cluster cluster = title.getCluster();
            outTitlesList.add(cluster == null ? -1 : cluster.getID(), title);
        }
        outTitlesMap = null; // free memory

        // read titles from file
        HashMap<Long, String> sentenceID2TitleMap = getTitlesYesterday();
        sentenceID2TitleMap.putAll(getTitlesToday());

        // write to file
        ClusterNodeFile clusterNodeFile = new ClusterNodeFile(conf.getHDFSFile("output"));
        clusterNodeFile.openWrite();
        for (Node title : outTitlesList.ascending().values()) {
            clusterwritable.set(title);
            clusterwritable.content = sentenceID2TitleMap.get(title.getID());
            clusterwritable.write(clusterNodeFile);
        }
        clusterNodeFile.closeWrite();
    }

    // add all node from non-expired clusters, i.e. containing a node that has not expired
    private void addNonExpiredClusters(HashMap<Long, Node> outTitlesMap,
            Cluster<NodeCount> cluster) throws IOException, InterruptedException {
        if (clusterNotExpired(cluster)) {
            for (Node title : cluster.getNodes()) {
                addRec(outTitlesMap, new HashSet(cluster.getNodes()));
            }
        }
    }

    // add all non-expired unclustered nodes
    private void addNonExpiredUnclusteredNodes(HashMap<Long, Node> outTitlesMap) {
        HashSet<Node> titlesToAdd = new HashSet();
        for (NodeCount title : clusteringGraph.getNodes().values()) {
            if (!title.isClustered() && nodeNotExpired(title) && !outTitlesMap.containsKey(title.getID())) {
                titlesToAdd.add(title);
            }
        }
        addRec(outTitlesMap, titlesToAdd);
    }

    /**
     * @param cluster
     * @return true if at least one of the cluster members was created after expireTime
     */
    public boolean clusterNotExpired(Cluster<NodeCount> cluster) {
        for (Node title : cluster.getNodes()) {
            if (nodeNotExpired(title)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param node
     * @return true if the node's creationtime is after expireTime
     */
    public boolean nodeNotExpired(Node node) {
        return node.getCreationTime() > expireTime;
    }

    public void addRec(HashMap<Long, Node> outTitlesMap, HashSet<Node> titlesToAdd) {
        while (titlesToAdd.size() > 0) {
            // add titlesToAdd to outTitlesMap
            for (Node title : titlesToAdd) {
                outTitlesMap.put(title.getID(), title);
            }
            
            // add the nearest neighbors of the added to titles as the next titlesToAdd
            // the || title.isClustered can probably be dropped
            HashSet<Node> newTitlesToAdd = new HashSet();
            for (Node title : titlesToAdd) {
                if (nodeNotExpired(title) || title.isClustered()) {
                    for (int nnIndex = 0; nnIndex < title.countNearestNeighbors(); nnIndex++) {
                        Node nn = title.getNearestNeighbor(nnIndex).getNode();
                        if (nn != null && !outTitlesMap.containsKey(nn.getID())) {
                            newTitlesToAdd.add(nn);
                        }
                    }
                }
            }
            titlesToAdd = newTitlesToAdd;
        }
    }

    /**
     * Read the clustering at the end of yesterday.
     */
    public void readClusters() throws IOException {
        // output points directly to a file, get the containing folder
        HDFSPath outfolder = conf.getHDFSPath("output").getParentPath();
        // YYYY-MM-DD of yesterday
        Date yesterday = DateTools.daysBefore(today, 1);
        // filename at the end of yesterday's clustering
        Datafile yesterdayDatafile = outfolder.getFile(DateTools.FORMAT.Y_M_D.format(yesterday));
        HashMapList<Integer, NodeCount> clusterNodes = new HashMapList();
        
        // file can be length=0 because there is a gap in the KBA data
        if (yesterdayDatafile.exists() && yesterdayDatafile.getLength() > 0) {
            
            // map of all nearest neighbor data, to be linked when all the nodes are read
            ArrayMap3<Long, ArrayList<Long>, ArrayList<Double>> nearestNeighbors = new ArrayMap3();
            
            ClusterNodeFile clusterNodeFile = new ClusterNodeFile(yesterdayDatafile);
            for (ClusterNodeWritable clusternode : clusterNodeFile) {
                NodeCount clusterNode = (NodeCount) clusteringGraph.getNode(clusternode.sentenceID);
                if (clusterNode == null) { // if does not exist, which should always be the case
                    clusterNode = getTitleNode(clusternode.sentenceID, clusternode.domain, clusternode.creationTime, clusternode.content);
                    nearestNeighbors.add(clusterNode.getID(), clusternode.getNearestNeighborIds(), clusternode.getNearestNeighborScores());
                    
                    // if clusterID < 0, node is not clustered, therefore no cluster is assigned
                    if (clusternode.clusterID >= 0) {
                        clusterNodes.add(clusternode.clusterID, clusterNode);
                    }
                } else {
                    log.info("clusterNode %d already exists, duplicates in file %s", clusterNode.getID(), yesterdayDatafile.getCanonicalPath());
                }
            }
            
            for (Map.Entry<Long, Tuple2<ArrayList<Long>, ArrayList<Double>>> nearestNeigbor : nearestNeighbors) {
                NodeCount title = clusteringGraph.getNode(nearestNeigbor.getKey());
                for (int i = 0; i < nearestNeigbor.getValue().key.size(); i++) {
                    long nearestNeighbordID = nearestNeigbor.getValue().key.get(i);
                    NodeCount nearestNeigborTitle = clusteringGraph.getNode(nearestNeighbordID);
                    Edge nearestNeigborEdge = new Edge(nearestNeigborTitle, nearestNeigbor.getValue().value.get(i));
                    title.add(nearestNeigborEdge);
                }
            }
        } else {
            // there is a gap in the data that exceeds the expiration interval (3 days)
            // therefore, there are no initial nodes in the graph, but we have to look
            // up where we should continue the cluster numbering, by looking further
            // back in time.
            clusteringGraph.setStartClusterID(findNextClusterID(outfolder, today));
        }

        // finally, create the clusters when a valid 2-degenerate core exists
        // and add the other assigned nodes (majority votes) as members.
        for (Map.Entry<Integer, ArrayList<NodeCount>> clusterid2Nodes : clusterNodes.entrySet()) {
            int clusterid = clusterid2Nodes.getKey();
            ArrayList<NodeCount> nodes = clusterid2Nodes.getValue();
            clusteringGraph.createClusterFromSet(clusterid, nodes);
        }
    }

    /**
     * @return the last clusterID found in previous cluster files
     */
    public static int findNextClusterID(HDFSPath graphClusterFolder, Date today) throws IOException {
        
        // find the last file in graphClusterFolder with a date (filename) before
        // today and a length > 0
        String mostRecentClusterFile = null;
        for (Datafile clusterDatafile : graphClusterFolder.getFiles()) {
            if (clusterDatafile.getLength() > 0) {
                try {
                    if (DateTools.FORMAT.Y_M_D.toDate(clusterDatafile.getName()).before(today)) {
                        mostRecentClusterFile = clusterDatafile.getName();
                    }
                } catch (ParseException ex) {
                    log.exception(ex, "findNextCluster invalid date %s", clusterDatafile);
                }
            }
        }
         
        // read the maximum cluster ID in that file if it exists.
        int maxClusterIdAlreadyUsed = -1;
        if (mostRecentClusterFile != null) {
            Datafile datafile = graphClusterFolder.getFile(mostRecentClusterFile);
            ClusterNodeFile clusterNodeFile = new ClusterNodeFile(datafile);
            for (ClusterNodeWritable title : clusterNodeFile) {
                maxClusterIdAlreadyUsed = Math.max(maxClusterIdAlreadyUsed, title.clusterID);
            }
        }
        
        // the next clusterID is the number to use for the next new cluster created
        return maxClusterIdAlreadyUsed + 1;
    }

    /**
     * @return map with all titles in the cluster file of yesterday
     */
    public HashMap<Long, String> getTitlesYesterday() {
        HashMap<Long, String> sentenceId2TitleMap = new HashMap();
        HDFSPath graphClusterFolder = conf.getHDFSPath("output").getParentPath();
        Date yesterday = DateTools.daysBefore(today, 1);

        Datafile datafile = graphClusterFolder.getFile(DateTools.FORMAT.Y_M_D.format(yesterday));
        if (datafile.exists()) {
            ClusterNodeFile clusterNodeFile = new ClusterNodeFile(datafile);
            for (ClusterNodeWritable title : clusterNodeFile) {
                sentenceId2TitleMap.put(title.sentenceID, title.content);
            }

        }
        return sentenceId2TitleMap;
    }

    /**
     * @return map with titles in the sentence file of today
     */
    public HashMap<Long, String> getTitlesToday() {
        HashMap<Long, String> sentenceId2TitleMap = new HashMap();
        HDFSPath graphClusterFolder = conf.getHDFSPath("input").getParentPath();

        Datafile datafile = graphClusterFolder.getFile(DateTools.FORMAT.Y_M_D.format(today));
        if (datafile.exists()) {
            SentenceFile clusterNodeFile = new SentenceFile(datafile);
            for (SentenceWritable cw : clusterNodeFile) {
                sentenceId2TitleMap.put(cw.sentenceID, cw.content);
            }
        }
        return sentenceId2TitleMap;
    }

    /**
     * @param sentenceid
     * @param domain
     * @param creationtime
     * @param content
     * @return If a node with sentenceID already exists it is returned, otherwise
     * a new titleNode is created, and added to the stream pool and inverted
     * index
     */
    public NodeCount getTitleNode(long sentenceid, int domain, long creationtime, String content) {
        NodeCount titleNode = (NodeCount) clusteringGraph.getNode(sentenceid);
        if (titleNode == null) {
            ArrayList<String> terms = tokenizer.tokenize(content);
            if (terms.size() > 1) {
                titleNode = new NodeCount(sentenceid, domain, creationtime, terms);
                clusteringGraph.getNodes().put(titleNode.getID(), titleNode);
                clusteringGraph.nodeStore.add(titleNode, terms);
            }
            log.info("getTitleNode %d %s %s", sentenceid, content, titleNode);
        }
        return titleNode;
    }

    private Date today(Context context) {
        // the name of the input file gives todays date
        String todayString = ContextTools.getInputPath(context).getName();
        try {
            return DateTools.FORMAT.Y_M_D.toDate(todayString);
        } catch (ParseException ex) {
            log.fatalexception(ex, "illegal date %s", todayString);
            return null;
        }
    }
}
