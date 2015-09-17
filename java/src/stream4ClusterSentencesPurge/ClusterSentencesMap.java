package stream4ClusterSentencesPurge;

import static io.github.k3nn.ClusteringGraph.getUnstemmedTokenizer;
import io.github.htools.lib.Log;
import java.io.IOException;
import kba1SourceToSentences.NewsDomains;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import sentence.SentenceWritable;
import io.github.htools.collection.HashMap3;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.hadoop.io.LongBoolWritable;
import io.github.htools.type.KV;
import kba1SourceToSentences.TitleFilter;
import static stream4ClusterSentences.ClusterSentencesJob.getRelevantDocs;

/**
 *
 * @author jeroen
 */
public class ClusterSentencesMap extends Mapper<LongWritable, SentenceWritable, LongBoolWritable, SentenceWritable> {

    public static final Log log = new Log(ClusterSentencesMap.class);
    Configuration conf;
    static DefaultTokenizer tokenizer = getUnstemmedTokenizer();
    NewsDomains domain = NewsDomains.getInstance();
    // TREC UUID, creation time, isCandidate
    HashMap3<String, Long, Boolean> relevantdocs;
    // key: sentenceid and isCandidate
    LongBoolWritable outkey = new LongBoolWritable();

    enum Counter {

        candidate,
        noncandidate
    }

    @Override
    public void setup(Context context) throws IOException {
        conf = context.getConfiguration();
        relevantdocs = getRelevantDocs(conf);
    }

    @Override
    public void map(LongWritable key, SentenceWritable sentence, Context context) throws IOException, InterruptedException {
        KV<Long, Boolean> docparams = relevantdocs.get(sentence.getDocumentID());
        if (docparams != null) {
            if (sentence.sentenceNumber != 0) { // row 0 is duplicate for extracted title -1
                if (sentence.sentenceNumber == -1) {
                    sentence.sentenceNumber = 0;
                    String urlHostPart = domain.getHostPart(sentence.domain);
                    sentence.content = TitleFilter.filterHost(urlHostPart, sentence.content);
                }
                outkey.set(sentence.sentenceID, docparams.value);
                context.write(outkey, sentence);
                if (docparams.value) {
                    context.getCounter(Counter.candidate).increment(1);
                } else {
                    context.getCounter(Counter.noncandidate).increment(1);
                }
            }
        }
    }
}
