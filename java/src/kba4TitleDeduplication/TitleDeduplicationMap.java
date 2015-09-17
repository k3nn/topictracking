package kba4TitleDeduplication;

import sentence.SentenceWritable;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import io.github.htools.hadoop.io.IntLongWritable;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author jeroen
 */
public class TitleDeduplicationMap extends Mapper<LongWritable, SentenceWritable, IntLongWritable, SentenceWritable> {

    public static final Log log = new Log(TitleDeduplicationMap.class);
    // grouped on hashcode and then sorted on creationtime
    IntLongWritable outkey = new IntLongWritable();

    @Override
    public void map(LongWritable key, SentenceWritable sentence, Context context) throws IOException, InterruptedException {
        // construct a key that sends titles with the same domain-title to the
        // same reducer, sorted on timestamp.
        int hashKey = MathTools.hashCode(sentence.domain, sentence.content.hashCode());
        outkey.set(hashKey, sentence.creationtime);
        
        context.write(outkey, sentence);
    }

}
