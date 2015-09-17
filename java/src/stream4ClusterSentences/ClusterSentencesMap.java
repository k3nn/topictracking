package stream4ClusterSentences;

import io.github.htools.lib.Log;
import java.io.IOException;
import kba1SourceToSentences.NewsDomains;
import kba1SourceToSentences.TitleFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import sentence.SentenceWritable;
import io.github.htools.collection.HashMap3;
import io.github.htools.hadoop.io.LongBoolWritable;
import io.github.htools.type.KV;

/**
 *
 * @author jeroen
 */
public class ClusterSentencesMap extends Mapper<LongWritable, SentenceWritable, LongBoolWritable, SentenceWritable> {

    public static final Log log = new Log(ClusterSentencesMap.class);
    Configuration conf;
    NewsDomains newsDomains = NewsDomains.getInstance();
    HashMap3<String, Long, Boolean> relevantDocuments;
    LongBoolWritable outkey = new LongBoolWritable();

    enum Counter {
        candidate,
        noncandidate
    }
    
    @Override
    public void setup(Context context) throws IOException {
        conf = context.getConfiguration();
        relevantDocuments = ClusterSentencesJob.getRelevantDocs(conf);
    }

    @Override
    public void map(LongWritable key, SentenceWritable sentence, Context context) throws IOException, InterruptedException {
        KV<Long, Boolean> documentProps = relevantDocuments.get(sentence.getDocumentID());
        if (documentProps != null) {
            if (sentence.sentenceNumber != -1) { // row 0 is duplicate for extracted title -1
                if (sentence.sentenceNumber == -1) {
                    sentence.sentenceNumber = 0;
                    String host = newsDomains.getHostPart(sentence.domain);
                    
                    // strip non-content from title sentences, e.g. "Yahoo News"
                    sentence.content = TitleFilter.filterHost(host, sentence.content);
                }
                // indicates whether this is the latest arriving document, so that
                // we can select sentences to add to the summary
                boolean isCandidate = documentProps.value;
                outkey.set(sentence.sentenceID, isCandidate);
                context.write(outkey, sentence);
                
                if (documentProps.value) {
                    context.getCounter(Counter.candidate).increment(1);
                } else {
                    context.getCounter(Counter.noncandidate).increment(1);
                }
            }
        }
    }
}
