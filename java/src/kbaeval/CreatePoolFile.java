package kbaeval;

import matchingClusterNode.MatchingClusterNodeFile;
import matchingClusterNode.MatchingClusterNodeWritable;
import io.github.htools.collection.ArrayMap;
import io.github.htools.io.Datafile;
import io.github.htools.lib.ArgsParser;
import io.github.htools.lib.Log;
import io.github.htools.lib.StrTools;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

public class CreatePoolFile {

    private static final Log log = new Log(CreatePoolFile.class);
    MatchingClusterNodeWritable recordcluster = new MatchingClusterNodeWritable();
    MatchingClusterNodeFile clusterfile;

    public CreatePoolFile(Datafile in, Datafile outPoolDatafile, Datafile matchfile, Datafile existigPoolDatafile, ArrayList<Datafile> inematch) {
        HashMap<String, PoolWritable> existingPool = readExistingPoolFile(existigPoolDatafile);
        PoolFile outPoolFile = new PoolFile(outPoolDatafile);
        outPoolFile.openWrite();
        ArrayList<MatchEditWritable> matched = new ArrayList();
        clusterfile = new MatchingClusterNodeFile(in);
        HashMap<String, PoolWritable> pooled = new HashMap();
        for (MatchingClusterNodeWritable w : clusterfile) {
            PoolWritable record = new PoolWritable();
            record.update_id = w.documentID + "-" + w.sentenceNumber;
            record.query_id = w.clusterID;
            PoolWritable existingpooled = existingPool.get(record.update_id);
            if (existingpooled != null && existingpooled.query_id == record.query_id) {
                existingpooled.write(outPoolFile);
                pooled.put(existingpooled.update_id, existingpooled);
            } else {
                record.doc_id = w.documentID;
                record.sentence_id = w.sentenceNumber;
                record.update_id = w.documentID + "-" + w.sentenceNumber;
                record.update_len = StrTools.countIndexOf(w.content, ' ') + 2;
                record.update_text = w.content;
                record.write(outPoolFile);
                pooled.put(record.update_id, record);
            }
        }

        HashMap<String, HashMap<String, MatchEditWritable>> ematches = getEmatches(inematch, pooled, existingPool, outPoolFile);
        for (PoolWritable record : pooled.values()) {
            HashMap<String, MatchEditWritable> list = ematches.get(record.update_id);
            if (list != null) {
                for (MatchEditWritable match : list.values()) {
                    matched.add(match);
                }
            } else {
                MatchEditWritable match = new MatchEditWritable();
                match.query_id = record.query_id;
                match.update_id = record.update_id;
                match.match = record.update_text;
                matched.add(match);
            }
        }
        outPoolFile.closeWrite();
        Collections.sort(matched, new Sorter());
        MatchEditFile mf = new MatchEditFile(matchfile);
        mf.openWrite();
        for (MatchEditWritable m : matched) {
            m.write(mf);
        }
        mf.closeWrite();
    }

    class Sorter implements Comparator<MatchEditWritable> {

        @Override
        public int compare(MatchEditWritable o1, MatchEditWritable o2) {
            int comp = o1.query_id - o2.query_id;
            if (comp == 0) {
                comp = o1.update_id.compareTo(o2.update_id);
                if (comp == 0) {
                    comp = o1.nugget_id.compareTo(o2.nugget_id);
                }
            }
            return comp;
        }

    }

    public HashMap<String, PoolWritable> readExistingPoolFile(Datafile datafile) {
        HashMap<String, PoolWritable> updateid2PooledSentence = new HashMap();
        PoolFile poolFile = new PoolFile(datafile);
        for (PoolWritable pooledSentence : poolFile) {
            PoolWritable existingPooledSentence = updateid2PooledSentence.get(pooledSentence.update_id);
            if (existingPooledSentence != null) {
                log.info("duplicate %s %s %s %s", 
                        existingPooledSentence.query_id, 
                        existingPooledSentence.update_id, 
                        pooledSentence.query_id, 
                        pooledSentence.update_id);
            }
            updateid2PooledSentence.put(pooledSentence.update_id, pooledSentence);
        }
        return updateid2PooledSentence;
    }

    public HashMap<String, HashMap<String, MatchEditWritable>> getEmatches(ArrayList<Datafile> ins,
            HashMap<String, PoolWritable> updates, HashMap<String, PoolWritable> ePool, PoolFile poolfile) {
        HashMap<String, HashMap<String, MatchEditWritable>> results = new HashMap();
        for (Datafile in : ins) {
            MatchFile pf = new MatchFile(in);
            for (MatchWritable w : pf) {
                String id = w.update_id + w.nugget_id;
                HashMap<String, MatchEditWritable> list = results.get(w.update_id);
                if (list == null) {
                    list = new HashMap();
                    results.put(w.update_id, list);
                }
                MatchEditWritable existing = list.get(w.nugget_id);
                MatchEditWritable mw = new MatchEditWritable();
                mw.nugget_id = w.nugget_id;
                mw.query_id = w.query_id;
                mw.update_id = w.update_id;
                if (!updates.containsKey(w.update_id)) {
                    PoolWritable existingpooled = ePool.get(w.update_id);
                    if (existingpooled != null && existingpooled.query_id == mw.query_id) {
                        existingpooled.write(poolfile);
                        updates.put(existingpooled.update_id, existingpooled);
                    }
                }
                if (updates.containsKey(w.update_id)) {
                    String text = updates.get(w.update_id).update_text;
                    mw.match = text.substring(w.match_start, Math.min(w.match_end + 1, text.length()));
                }
                list.put(w.nugget_id, mw);
            }
        }
        return results;
    }

    public ArrayMap<Integer, String> getMatchedUpdates(ArrayList<Datafile> ins) {
        ArrayMap<Integer, String> results = new ArrayMap();
        for (Datafile in : ins) {
            MatchFile pf = new MatchFile(in);
            for (MatchWritable w : pf) {
                results.add(w.query_id, w.update_id);
            }
        }
        return results;
    }

    public static void main(String args[]) {
        ArgsParser ap = new ArgsParser(args, "-i input outpool outmatch existingpool {existingmatch}");
        Datafile in = new Datafile(ap.get("input"));
        Datafile out = new Datafile(ap.get("outpool"));
        Datafile outmatch = new Datafile(ap.get("outmatch"));
        Datafile inepool = new Datafile(ap.get("existingpool"));
        ArrayList<Datafile> inematch = new ArrayList();
        if (ap.exists("existingmatch")) {
            for (String s : ap.getStrings("existingmatch")) {
                inematch.add(new Datafile(s));
            }
        }
        new CreatePoolFile(in, out, outmatch, inepool, inematch);
    }
}
