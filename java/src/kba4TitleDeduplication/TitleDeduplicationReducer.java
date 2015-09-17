package kba4TitleDeduplication;

import sentence.SentenceWritable;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.io.IntLongWritable;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Remove titles, if the exact same title appeared on the same domain within a
 * week. the input is grouped by a hash key on domain-title, and sorted on
 * timestamp
 *
 * @author jeroen
 */
public class TitleDeduplicationReducer extends Reducer<IntLongWritable, SentenceWritable, NullWritable, SentenceWritable> {

    public static final Log log = new Log(TitleDeduplicationReducer.class);
    private long oneWeekInSeconds = 7 * 24 * 60 * 60;

    HashMap<Integer, HashMap<String, SentenceWritable>> domain2Sentences;

    public void setup(Context context) {
        domain2Sentences = new HashMap();
    }

    @Override
    public void reduce(IntLongWritable key, Iterable<SentenceWritable> sentences, Context context) throws IOException, InterruptedException {
        // collision map, on domain, then sentence.
        for (SentenceWritable sentence : sentences) {
            HashMap<String, SentenceWritable> sentencesPerDomain = domain2Sentences.get(sentence.domain);
            if (sentencesPerDomain == null) {
                sentencesPerDomain = new HashMap();
                domain2Sentences.put(sentence.domain, sentencesPerDomain);
            }

            //
            SentenceWritable matchingSentence = sentencesPerDomain.get(sentence.content);
            if (matchingSentence == null) {
                // a sentence in the same domain with the same title does not exists yet
                sentencesPerDomain.put(sentence.content, sentence.clone());
            } else {
                // a sentence in the same domain with the same title exists
                // if more than a week passed, write it, and replace with new sentence
                if (sentence.creationtime - matchingSentence.creationtime > oneWeekInSeconds) {
                    context.write(NullWritable.get(), matchingSentence);
                    sentencesPerDomain.put(sentence.content, sentence.clone());
                }
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (HashMap<String, SentenceWritable> sentencesPerDomain : domain2Sentences.values()) {
            for (SentenceWritable sentence : sentencesPerDomain.values()) {
                context.write(NullWritable.get(), sentence);
            }
        }
    }
}
