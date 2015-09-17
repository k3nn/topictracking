package kba7TopicMatchingSentences;

import io.github.k3nn.ClusteringGraph;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.io.IntLongWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import kbaeval.TopicWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import sentence.SentenceWritable;
import io.github.htools.collection.ArrayMap;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.lib.CollectionTools;

/**
 * Route titles that contain all terms to one topic, to the reducer of that
 * topic.
 *
 * @author jeroen
 */
public class TopicMatchingSentencesMap extends Mapper<LongWritable, SentenceWritable, IntLongWritable, SentenceWritable> {

    public static final Log log = new Log(TopicMatchingSentencesMap.class);
    Conf conf;
    public static DefaultTokenizer tokenizer = ClusteringGraph.getUnstemmedTokenizer();

    // topics to process
    public ArrayMap<TopicWritable, HashSet<String>> topics;

    // set of all query terms of all topics, for fast decision whether to match
    // a title to all individual topics
    public HashSet<String> allterms;

    // topic number & creation time
    IntLongWritable outkey = new IntLongWritable();

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        topics = TopicMatchingSentencesJob.getTopics(conf);
        allterms = TopicMatchingSentencesJob.allTopicTerms(topics.values());
    }

    @Override
    public void map(LongWritable key, SentenceWritable title, Context context) throws IOException, InterruptedException {
        ArrayList<String> titleTerms = tokenizer.tokenize(title.content);
        if (CollectionTools.containsAny(allterms, titleTerms)) {
            HashSet<String> titleterms = new HashSet(titleTerms);
            for (int topicIndex = 0; topicIndex < topics.size(); topicIndex++) {
                if (titleterms.containsAll(topics.get(topicIndex).getValue())) {
                    TopicWritable topic = topics.get(topicIndex).getKey();
                    if (title.creationtime >= topic.start && title.creationtime <= topic.end) {
                        outkey.set(topicIndex, title.creationtime);
                        context.write(outkey, title);
                    }
                }
            }
        }
    }
}
