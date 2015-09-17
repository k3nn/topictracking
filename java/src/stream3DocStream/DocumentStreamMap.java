package stream3DocStream;

import cluster.ClusterWritable;
import cluster.NodeWritable;
import io.github.htools.collection.ArrayMap;
import io.github.htools.collection.ArrayMap3;
import io.github.htools.collection.HashMap3;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import io.github.htools.type.KV;
import io.github.htools.type.Tuple2;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Transforms a stream of query matching clusters to a list of document IDs that
 * is used for the clustering of sentences contained in those documents. In the
 * resulting list of document IDs, the documents remain in the order they were
 * seen in the title clustering, removing document IDs that were seen previously
 * and marking candidate documents, i.e. documents that were clustered in a
 * query matching cluster immediately on arrival, since these are the only
 * documents we allow the select sentences from.
 *
 * @author jeroen
 */
public class DocumentStreamMap extends Mapper<LongWritable, ClusterWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(DocumentStreamMap.class);
    HashMap<Integer, ArrayList<ClusterWritable>> inmap = new HashMap();
    HashSet<String> documentsSeen = new HashSet();
    ArrayMap3<String, Long, Boolean> documentStreamList = new ArrayMap3();

    @Override
    public void map(LongWritable key, ClusterWritable cluster, Context context) throws IOException, InterruptedException {
        NodeWritable candidateTitle = cluster.getCandidateNode();
        for (NodeWritable title : cluster.nodes) {
            log.info("%s %b", title.docid, documentsSeen.contains(title.docid));
            if (documentsSeen.contains(title.docid)) {
                if (title == candidateTitle) {
                    log.fatal("wrong order %s %d", title.docid, cluster.clusterid);
                }
            } else {
                // add same result to both map and list, which allows to change the candidate flag
                // in case there are clusters with the same timestamps
                documentsSeen.add(title.docid);
                documentStreamList.add(title.docid, candidateTitle.creationtime, title == candidateTitle);
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException {
        DocumentStreamFile documentStreamFile = outputDocFile(context);
        documentStreamFile.openWrite();
        DocumentStreamWritable document = new DocumentStreamWritable();
        for (Map.Entry<String, Tuple2<Long, Boolean>> entry : documentStreamList) {
            document.docid = entry.getKey();
            document.creationtime = entry.getValue().key;
            document.isCandidate = entry.getValue().value;
            document.write(documentStreamFile);
        }
        documentStreamFile.closeWrite();
    }

    public DocumentStreamFile outputDocFile(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        String topicFilename = ContextTools.getInputPath(context).getName();
        HDFSPath documentStreamFolder = new HDFSPath(conf, conf.get("output"));
        return new DocumentStreamFile(documentStreamFolder.getFile(topicFilename));
    }
}
