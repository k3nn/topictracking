package stream4ClusterSentencesPurge.IDF;

import cluster.ClusterFile;
import io.github.k3nn.NodeStoreII;
import io.github.k3nn.impl.NodeStoreIIIDF;
import io.github.k3nn.impl.NodeMagnitude;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.collection.HashMapInt;
import io.github.htools.hadoop.Conf;
import java.io.IOException;
import stream4ClusterSentencesPurge.ClusterSentencesReducer.SentenceClusteringGraph;

/**
 * Modifies the ClusteringGraph to use NodeStoreIIIDF, and reads the document frequencies
 * from the file configured as "idf".
 * @author jeroen
 */
public class ClusterSentencesReducer extends stream4ClusterSentencesPurge.ClusterSentencesReducer {

    public static final Log log = new Log(ClusterSentencesReducer.class);

    public SentenceClusteringGraph getClusteringGraph()  throws IOException {
        return new ClusteringGraphIDF(conf, clusterFile);
    }

    public static class ClusteringGraphIDF<N extends NodeMagnitude> extends SentenceClusteringGraph<N> {

        public ClusteringGraphIDF(Conf conf, ClusterFile clusterFile) throws IOException {
            super(clusterFile);
            nodeStore = createII(conf);
        }
        
        public NodeStoreII createII(Conf conf) throws IOException {
            Datafile datafile = conf.getHDFSFile("idf");
            datafile.setBufferSize((int)datafile.getLength());
            HashMapInt<String> term2DocFreq = new HashMapInt();
            String lines[] = datafile.readAsString().split("\\n");
            for (String line : lines) {
                String[] linePart = line.split("\\s");
                String term = linePart[0];
                int documentFrequency = Integer.parseInt(linePart[1]);
                term2DocFreq.put(term, documentFrequency);
            }
            return new NodeStoreIIIDF(term2DocFreq.get("#"), term2DocFreq);
        }
     }
}
