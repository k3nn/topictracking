package stream2AddDocID;

import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import sentence.SentenceWritable;
import io.github.htools.collection.HashMapList;
import io.github.htools.hadoop.ContextTools;

/**
 * emit sentences that match a node in a query matching cluster, to the reducer of that topic
 * @author jeroen
 */
public class ClusterDocidMap extends Mapper<LongWritable, SentenceWritable, IntWritable, SentenceWritable> {

    public static final Log log = new Log(ClusterDocidMap.class);
    Conf conf;
    
    // these are the targeted sentenceIds that appear in the queryMatchingClusters
    HashMapList<Long, Integer> sentenceId2TopicReducer;
    IntWritable outkey = new IntWritable();

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        sentenceId2TopicReducer = ClusterDocidJob.getTargetedSentenceIds(conf);
    }

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) throws IOException, InterruptedException {
        ArrayList<Integer> topicReducers = sentenceId2TopicReducer.get(value.sentenceID);
        if (topicReducers != null) {
            for (int reducer : topicReducers) {
                outkey.set(reducer);
                context.write(outkey, value);
            }
        }
    }
}
