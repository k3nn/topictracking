package stream5Retrieve;

import matchingClusterNode.MatchingClusterNodeFile;
import matchingClusterNode.MatchingClusterNodeWritable;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.io.HDFSPath;
import static io.github.htools.lib.PrintTools.sprintf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import kbaeval.TrecFile;
import kbaeval.TrecWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import stream5Retrieve.RetrieveJob.Setting;

/**
 * Writes the resulting summary of qualified sentences to a TrecFile (TREC
 * format results) and additionally a MatchingClusterNodeFile containing the
 * actual content for inspection.
 *
 * @author jeroen
 */
public class RetrieveReducer extends Reducer<Setting, MatchingClusterNodeWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(RetrieveReducer.class);
    Conf conf;
    Setting setting;
    HDFSPath outPath;
    ArrayList<MatchingClusterNodeWritable> results;

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        outPath = conf.getHDFSPath("output");
        results = new ArrayList();
    }

    @Override
    public void reduce(Setting setting, Iterable<MatchingClusterNodeWritable> qualifiedSentences, Context context) throws IOException, InterruptedException {
        this.setting = setting;
        for (MatchingClusterNodeWritable qualifiedSentence : qualifiedSentences) {
            results.add(qualifiedSentence.clone());
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        if (results != null && results.size() > 0) {
            Collections.sort(results, new Sorter());
            TrecFile trecFile = openTrecFile(setting);
            MatchingClusterNodeFile outClusterFile = openSentenceFile(setting);
            TrecWritable record = new TrecWritable();
            for (MatchingClusterNodeWritable t : results) {
                //log.info("%d %s", t.creationTime, t.documentID);
                t.write(outClusterFile);
                record.document = t.documentID;
                record.timestamp = t.creationTime;
                record.topic = t.clusterID;
                record.sentence = t.sentenceNumber;
                record.write(trecFile);
            }
            trecFile.closeWrite();
            outClusterFile.closeWrite();
        }
    }

    public TrecFile openTrecFile(Setting setting) {
        Datafile trecDatafile = outPath.getFile(getFilename(setting));
        TrecFile trecFile = new TrecFile(trecDatafile);
        trecFile.openWrite();
        return trecFile;
    }
    
    public MatchingClusterNodeFile openSentenceFile(Setting setting) {
        Datafile clusterDatafile = outPath.getFile(getFilename(setting) + ".title");
        MatchingClusterNodeFile outClusterFile = new MatchingClusterNodeFile(clusterDatafile);
        outClusterFile.openWrite();
        return outClusterFile;
    }

    public String getFilename(Setting setting) {
        return sprintf("results.%d.%d.%d.%d", (int) (100 * setting.gainratio), (int) (10 * setting.hours), setting.length, setting.topk);
    }
    
    class Sorter implements Comparator<MatchingClusterNodeWritable> {

        @Override
        public int compare(MatchingClusterNodeWritable o1, MatchingClusterNodeWritable o2) {
            long comp = o1.clusterID - o1.clusterID;
            if (comp == 0) {
                comp = o1.creationTime - o2.creationTime;
            }
            return (comp < 0) ? -1 : (comp > 0) ? 1 : 0;
        }

    }
}
