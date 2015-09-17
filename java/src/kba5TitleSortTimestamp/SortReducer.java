package kba5TitleSortTimestamp;

import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import sentence.SentenceWritable;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.hadoop.io.DayPartitioner;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import sentence.SentenceFile;
import io.github.htools.hadoop.Conf;

/**
 * Set the sentenceID of incoming sentences based on the number of days since
 * EPOCH_START and a unique sequence number, and write to file in chronological
 * order.
 *
 * @author jeroen
 */
public class SortReducer extends Reducer<LongWritable, SentenceWritable, NullWritable, SentenceWritable> {

    public static final Log log = new Log(SortReducer.class);
    SentenceFile sentenceFile;
    int sequence = 0;

    @Override
    public void setup(Context context) throws IOException {
        Conf conf = ContextTools.getConfiguration(context);
        String date = DayPartitioner.getDate(context);
        HDFSPath outFolder = conf.getHDFSPath("output");
        Datafile outDatafile = outFolder.getFile(date);
        sentenceFile = new SentenceFile(outDatafile);
        sentenceFile.openWrite();
    }

    @Override
    public void reduce(LongWritable key, Iterable<SentenceWritable> sentences, Context context) throws IOException, InterruptedException {
        for (SentenceWritable sentence : sentences) {
            sentence.setSentenceID(sentence.creationtime, sequence++);
            sentence.write(sentenceFile);
        }
    }

    @Override
    public void cleanup(Context context) {
        sentenceFile.closeWrite();
    }
}
