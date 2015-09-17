package kba1SourceToSentences;

import sentence.SentenceWritable;
import io.github.htools.hadoop.io.IntLongIntWritable;
import io.github.htools.lib.Log;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Assigns a unique sequence ID to sentences and writes SentenceWritables to file.
 * Actually figured out that using Hadoop's OutputFormat possibly messes up the order of 
 * the output records, so to ensure replicable experiments should write ourselves.
 * @author jeroen
 */
public class SourceToSentenceReducer extends Reducer<IntLongIntWritable, SentenceWritable, NullWritable, SentenceWritable> {

    public static final Log log = new Log(SourceToSentenceReducer.class);
    // unique sentence sequence id per file (so per date)
    int sequence = 0;
    
    @Override
    public void reduce(IntLongIntWritable key, Iterable<SentenceWritable> sentences, Context context) throws IOException, InterruptedException {
        for (SentenceWritable sentence : sentences) {
            sentence.setSentenceID(sentence.getDaysSinceEpoch(), sequence++);
            context.write(NullWritable.get(), sentence);
        }
    }
}
