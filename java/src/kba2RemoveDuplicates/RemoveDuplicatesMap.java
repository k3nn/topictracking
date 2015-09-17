package kba2RemoveDuplicates;

import sentence.SentenceWritable;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import kba1SourceToSentences.NewsDomains;
import kba1SourceToSentences.TitleFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import io.github.htools.hadoop.io.LongLongWritable;

/**
 * Removes duplicates that have the exact same documentID in the collection,
 * within a document duplicate sentences that have the same sentence number, and
 * strips the extracted title from non-title elements.
 *
 * @author jeroen
 */
public class RemoveDuplicatesMap extends Mapper<LongWritable, SentenceWritable, LongLongWritable, SentenceWritable> {

    public static final Log log = new Log(RemoveDuplicatesMap.class);
    NewsDomains domain = NewsDomains.getInstance();
    HashMap<Integer, SentenceWritable> docSentences = new HashMap();
    HashSet<String> docIdsSeen = new HashSet();
    String currentDocID = "";
    LongLongWritable outkey = new LongLongWritable();

    @Override
    public void map(LongWritable key, SentenceWritable sentence, Context context) throws IOException, InterruptedException {
        if (!sentence.getDocumentID().equals(currentDocID)) {
            writeDocument(context);
            currentDocID = sentence.getDocumentID();
            docSentences = new HashMap();
        }
        docSentences.put(sentence.sentenceNumber, sentence);
    }

    public void writeDocument(Context context) throws IOException, InterruptedException {
        if (docSentences.size() > 0) {
            SentenceWritable lastSentence = docSentences.get(-1);
            if (lastSentence != null) {
                // skip duplicate docids
                if (docIdsSeen.contains(currentDocID)) {
                    return;
                }
                docIdsSeen.add(currentDocID);
            }
            TreeMap<Integer, SentenceWritable> sorted = new TreeMap(docSentences);
            for (SentenceWritable sentence : sorted.values()) {
                // -1 is the extracted title
                if (sentence.sentenceNumber == -1) {
                    String dom = domain.getHostPart(sentence.domain);
                    // remove domain specific non-information from the extracted title
                    sentence.content = TitleFilter.filterHost(dom, sentence.content);
                }
                outkey.set(sentence.creationtime, sentence.sentenceID);
                context.write(outkey, sentence);
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        writeDocument(context);
    }
}
