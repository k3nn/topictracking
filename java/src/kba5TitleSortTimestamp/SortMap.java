package kba5TitleSortTimestamp;

import sentence.SentenceWritable;
import io.github.htools.lib.Log;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Output all read sentences with a creationtime key for partitioning and sorting.
 * @author jeroen
 */
public class SortMap extends Mapper<LongWritable, SentenceWritable, LongWritable, SentenceWritable> {

    public static final Log log = new Log(SortMap.class);
    LongWritable creationtime = new LongWritable();

    @Override
    public void map(LongWritable key, SentenceWritable sentence, Context context) throws IOException, InterruptedException {
        creationtime.set(sentence.creationtime);
        context.write(creationtime, sentence);
    }

}
