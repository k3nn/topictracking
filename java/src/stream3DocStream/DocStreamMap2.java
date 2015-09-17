package stream3DocStream;

import cluster.ClusterWritable;
import cluster.NodeWritable;
import io.github.htools.collection.ArrayMap3;
import io.github.htools.collection.HashMap3;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import io.github.htools.type.KV;
import io.github.htools.type.Tuple2;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Transforms a stream of query matching clusters to a list of document IDs that
 * is used for the clustering of sentences contained in those documents. In the
 * resulting list of document IDs, the documents remain in the order they were seen
 * in the title clustering, removing document IDs that were seen previously and
 * marking candidate documents, i.e. documents that were clustered in a query matching
 * cluster immediately on arrival, since these are the only documents we allow 
 * the select sentences from.
 * 
 * @author jeroen
 */
public class DocStreamMap2 extends Mapper<LongWritable, ClusterWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(DocStreamMap2.class);
    HashMap<Integer, ArrayList<ClusterWritable>> inmap = new HashMap();

    enum Counters {

        candidate,
        noncandidate
    }

    @Override
    public void map(LongWritable key, ClusterWritable value, Context context) throws IOException, InterruptedException {
        ArrayList<ClusterWritable> list = inmap.get(value.clusterid);
        if (list == null) {
            list = new ArrayList();
            inmap.put(value.clusterid, list);
        }
        list.add(value);
    }

    @Override
    public void cleanup(Context context) throws IOException {
        HashMap3<String, Long, Boolean> out = new HashMap3();
        for (ArrayList<ClusterWritable> list : inmap.values()) {
            //Collections.sort(list, Sorter.instance);
            for (int i = 0; i < list.size(); i++) {
                ClusterWritable current = list.get(i);
                Collections.sort(current.nodes, SorterUrl.instance);
                long emittime = current.getCandidateNode().creationtime;
                for (NodeWritable url : current.nodes) {
                    if (!out.containsKey(url)) {
                        out.put(url.docid, url.creationtime, i == (list.size() - 1));
                        if (i == (list.size() - 1)) {
                            context.getCounter(Counters.candidate).increment(1);
                        } else {
                            context.getCounter(Counters.noncandidate).increment(1);
                        }
                    }
                }
            }
        }
        DocumentStreamFile df = outputFile(context);
        df.openWrite();
        DocumentStreamWritable record = new DocumentStreamWritable();
        ArrayMap3<Long, Boolean, String> sorted = new ArrayMap3();
        for (Map.Entry<String, KV<Long, Boolean>> entry : out.entrySet()) {
            sorted.add(entry.getValue().key, entry.getValue().value, entry.getKey());
        }
        for (Map.Entry<Long, Tuple2<Boolean, String>> entry : sorted.ascending()) {
            record.docid = entry.getValue().value;
            record.creationtime = entry.getKey();
            record.isCandidate = entry.getValue().key;
            record.write(df);
        }
        df.closeWrite();
    }

    public DocumentStreamFile outputFile(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        String filename = ContextTools.getInputPath(context).getName();
        HDFSPath outdir = new HDFSPath(conf, conf.get("output"));
        return new DocumentStreamFile(outdir.getFile(filename));
    }

    static class Sorter implements Comparator<ClusterWritable> {

        static Sorter instance = new Sorter();

        @Override
        public int compare(ClusterWritable o1, ClusterWritable o2) {
            int comp = (int) (o1.getCandidateNode().creationtime - o2.getCandidateNode().creationtime);
            if (comp == 0) {
                comp = o1.nodes.size() - o2.nodes.size();
            }
            return comp;
        }
    }

    static class SorterUrl implements Comparator<NodeWritable> {

        static SorterUrl instance = new SorterUrl();

        @Override
        public int compare(NodeWritable o1, NodeWritable o2) {
            return (int) (o1.creationtime - o2.creationtime);
        }
    }
}
