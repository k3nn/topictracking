package kba2RemoveDuplicates;

import sentence.SentenceFile;
import sentence.SentenceWritable;
import io.github.htools.hadoop.io.DayPartitioner;
import io.github.htools.lib.Log;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Remove titles, if the exact same title appeared on the same domain within a week.
 * the input is grouped by a hash key on domain-title, and sorted on timestamp
 * @author jeroen
 */
public class RemoveDuplicatesReducer extends Reducer<Object, SentenceWritable, NullWritable, SentenceWritable> {

    public static final Log log = new Log(RemoveDuplicatesReducer.class);
    SentenceFile sentenceFile;
    int sequence = 0;

    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        String date = DayPartitioner.getDate(context);
        HDFSPath outdir = new HDFSPath(conf, conf.get("output"));
        Datafile dataFile = outdir.getFile(date);
        sentenceFile = new SentenceFile(dataFile);
        sentenceFile.openWrite();
    }
    
    @Override
    public void reduce(Object key, Iterable<SentenceWritable> sentences, Context context) throws IOException, InterruptedException {
        for (SentenceWritable sentence : sentences) {
            // assign a new unique sentence ID to the sentence
            sentence.setSentenceID(sentence.creationtime, sequence++);
            sentence.write(sentenceFile);
        }
     }
    
    @Override
    public void cleanup(Context context) {
        sentenceFile.closeWrite();
    }    
}
