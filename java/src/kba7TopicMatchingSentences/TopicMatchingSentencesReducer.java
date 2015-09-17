package kba7TopicMatchingSentences;

import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import static io.github.htools.lib.PrintTools.sprintf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.hadoop.io.IntLongWritable;
import java.io.IOException;
import kbaeval.TopicWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import sentence.SentenceFile;
import sentence.SentenceWritable;
import io.github.htools.collection.ArrayMap;
import io.github.htools.hadoop.Conf;
import java.util.HashSet;
import static kba7TopicMatchingSentences.TopicMatchingSentencesJob.getTopics;

/**
 * Write all titles that contain all terms for the given topic.
 * @author jeroen
 */
public class TopicMatchingSentencesReducer extends Reducer<IntLongWritable, SentenceWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(TopicMatchingSentencesReducer.class);
    SentenceFile sentenceFile;

    @Override
    public void setup(Context context) throws IOException {
        Conf conf = ContextTools.getConfiguration(context);
        HDFSPath outPath = conf.getHDFSPath("output");
        Datafile outDatafile = outPath.getFile(sprintf("topic.%d", this.getTopicID(context)));
        sentenceFile = new SentenceFile(outDatafile);
    }

    @Override
    public void reduce(IntLongWritable key, Iterable<SentenceWritable> sentences, Context context) throws IOException, InterruptedException {
        for (SentenceWritable sentence : sentences) {
            sentence.write(sentenceFile);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        sentenceFile.closeWrite();
    }

    /**
     * @param context
     * @return TopicID based on reducer number and topics stored in the Configuration
     * by the Job.
     */
    public int getTopicID(Context context) {
        int reducer = ContextTools.getTaskID(context);
        ArrayMap<TopicWritable, HashSet<String>> topics = getTopics(context.getConfiguration());
        return topics.get(reducer).getKey().id;
    }
}
