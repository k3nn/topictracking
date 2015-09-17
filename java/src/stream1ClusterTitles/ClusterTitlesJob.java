package stream1ClusterTitles;

import cluster.ClusterWritable;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.IntPartitioner;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import static io.github.htools.lib.PrintTools.sprintf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import sentence.SentenceFile;
import sentence.SentenceInputFormat;
import sentence.SentenceWritable;
import io.github.htools.hadoop.io.IntLongIntWritable;

/**
 * This simulates a streaming process of the collection in parallel, by taking
 * the snapshot of yesterday midnight, and then processing the arrivals of a
 * single day as a stream.
 *
 * During the streaming, the clusters are monitored, whether they contain a
 * sentence that is targeted by a query. The output consists of cluster snapshot
 * for every sentence added to a cluster that (then) contains a targeted
 * sentence. The snapshot contains all sentences the cluster consists of at that
 * point in time, the last sentence being the candidate Node that can qualify
 * for emission to the user.
 * <p/>
 * To make it easier, this routine uses an inventory of the sentences that
 * contain all query terms, for instance created by kba7TopicMatchingSentences.
 * <p/>
 * input: folder of SentenceFile with chronologically sorted titles (kba5)
 * clusters: folder of ClusterNodeFile containing midnight snapshots of title
 * clustering (kba6) 
 * topicmatchingsentences: folder with SentenceFile per topic
 * contain the titles that contain all query terms for that topic 
 * output: folder
 * with ClusterFile per topic containing all title clusters that were formed
 * that contain a query matching title.
 *
 * @author jeroen
 */
public class ClusterTitlesJob {

    private static final Log log = new Log(ClusterTitlesJob.class);
    private static final String TOPICSENTENCES = "topicsentences";
    private static final String TOPICSENTENCEID = "topicsentence.key";
    private static final String TOPICSENTENCEREDUCER = "topicsentence.value";

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "-i input -c clusters -o output -t topicmatchingsentences");
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(1800000);
        conf.setMapSpeculativeExecution(false);
        conf.setReduceSpeculativeExecution(false);
        storeTopicMatchingSentences(conf);

        int reducers = getReducers(conf);

        Job job = new Job(conf,
                conf.get("input"),
                conf.get("clusters"),
                conf.get("topicmatchingsentences"),
                conf.get("output"));

        job.setInputFormatClass(SentenceInputFormat.class);
        SentenceInputFormat.addDirs(job, conf.get("input"));
        SentenceInputFormat.setNonSplitable(job);

        job.setNumReduceTasks(reducers);
        job.setMapperClass(ClusterTitlesMap.class);
        job.setMapOutputKeyClass(IntLongIntWritable.class);
        job.setMapOutputValueClass(ClusterWritable.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.setPartitionerClass(IntPartitioner.class);
        job.setSortComparatorClass(IntLongIntWritable.SortComparator.class);
        job.setReducerClass(ClusterTitlesReducer.class);

        job.waitForCompletion(true);
    }

    /**
     * store the topics in the test set, with a list of sentence IDs of titles
     * that contain all query terms, in the configuration
     *
     * @param conf
     */
    public static void storeTopicMatchingSentences(Conf conf) throws IOException {
        HashMap<Long, ArrayList<Integer>> sentence2TopicReducer = readTopicMatchingSentences(conf);
        conf.setInt(TOPICSENTENCES, sentence2TopicReducer.size());
        int sentenceIndex = 0;
        for (Map.Entry<Long, ArrayList<Integer>> sentence2reducer : sentence2TopicReducer.entrySet()) {
            conf.setLong(sprintf("%s.%d", TOPICSENTENCEID, sentenceIndex), sentence2reducer.getKey());
            conf.setIntList(sprintf("%s.%d", TOPICSENTENCEREDUCER, sentenceIndex), sentence2reducer.getValue());
            sentenceIndex++;
        }
    }

    /**
     * @param conf
     * @return map from sentenceID to reducer numbers that will reduce all clusters
     * for the corresponding topic (i.e. one reducer per topic), which is being constructed
     * from the topicMatchingSentences as already determined by kba7. Reducer 0
     * will reduce the first topic in the topicMatchingSentences files, etc.
     */
    public static HashMap<Long, ArrayList<Integer>> getTopicMatchingSentences(Configuration conf) throws IOException {
        int numberOfSentences = conf.getInt(TOPICSENTENCES, 0);
        HashMap<Long, ArrayList<Integer>> sentence2TopicReducer = new HashMap(numberOfSentences);
        for (int sentenceIndex = 0; sentenceIndex < numberOfSentences; sentenceIndex++) {
            long sentenceID = conf.getLong(sprintf("%s.%d", TOPICSENTENCEID, sentenceIndex), 0);
            ArrayList<Integer> reducerList = Conf.getIntList(conf, sprintf("%s.%d", TOPICSENTENCEREDUCER, sentenceIndex));
            sentence2TopicReducer.put(sentenceID, reducerList);
        }
        return sentence2TopicReducer;
    }

    public static int getReducers(Conf conf) throws IOException {
        return getTopicMatchingSentenceFiles(conf).size();
    }

    /**
     *
     * @param conf
     * @return map from sentenceID to reducer numbers that will reduce all clusters
     * for the corresponding topic (i.e. one reducer per topic), which is being constructed
     * from the topicMatchingSentences as already determined by kba7. Reducer 0
     * will reduce the first topic in the topicMatchingSentences files, etc.
     * @throws IOException
     */
    private static HashMap<Long, ArrayList<Integer>> readTopicMatchingSentences(Conf conf) throws IOException {
        HashMap<Long, ArrayList<Integer>> sentence2TopicReducer = new HashMap();
        ArrayList<Datafile> files = getTopicMatchingSentenceFiles(conf);
        for (int reducer = 0; reducer < files.size(); reducer++) {
            Datafile df = files.get(reducer);
            SentenceFile sentenceFile = new SentenceFile(df);
            for (SentenceWritable sentence : sentenceFile) {
                ArrayList<Integer> reducerList = sentence2TopicReducer.get(sentence.sentenceID);
                if (reducerList == null) {
                    reducerList = new ArrayList();
                    sentence2TopicReducer.put(sentence.sentenceID, reducerList);
                }
                reducerList.add(reducer);
            }
        }
        return sentence2TopicReducer;
    }

    /**
     * @param conf
     * @return a list of files per topic, that contain sentence IDs of titles
     * containing all query terms, generated by TopicMatchingSentences
     * @throws IOException
     */
    public static ArrayList<Datafile> getTopicMatchingSentenceFiles(Conf conf) throws IOException  {
        HDFSPath path = new HDFSPath(conf, conf.get("topicmatchingsentences"));
        return path.getFiles("*.\\d+");
    }
}
