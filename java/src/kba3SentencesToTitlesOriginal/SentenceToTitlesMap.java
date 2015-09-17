package kba3SentencesToTitlesOriginal;

import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import sentence.SentenceWritable;
import io.github.htools.lib.Log;
import java.io.IOException;
import kba1SourceToSentences.NewsDomains;
import kba1SourceToSentences.TitleFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import sentence.SentenceFile;
import io.github.htools.collection.HashPair;
import io.github.htools.hadoop.ContextTools;

/**
 * Filter out all but the first sentence from the KBA corpus, i.e. the first
 * sentence that was returned by the TREC organizers (i.e. row=0). 
 * We don't use these, since these sentences are often misparsed to contain
 * a lot more than just the title. This version was used to experiment with
 * clustering these nevertheless.
 * @author jeroen
 */
public class SentenceToTitlesMap extends Mapper<LongWritable, SentenceWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(SentenceToTitlesMap.class);
    NewsDomains domain = NewsDomains.getInstance();
    SentenceFile sentenceFile;
    // track titles seen per domain, so we don't have two document with the same
    // title in the same domain, which is likely a duplicate document
    HashPair<String, Integer> titlesSeen = new HashPair();
    long creationtime = 0;

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        // date is the filename of the inputfile
        String date = ContextTools.getInputPath(context).getName();
        HDFSPath outdir = new HDFSPath(conf, conf.get("output"));
        Datafile datafile = outdir.getFile(date);
        sentenceFile = new SentenceFile(datafile);
        sentenceFile.openWrite();
    }

    @Override
    public void map(LongWritable key, SentenceWritable sentence, Context context) {
        try {
             // use only sentenceNumber 0, which is the first sentence (title)
            // supplied by TREC organizers in the KBA corpus
            if (sentence.sentenceNumber == 0) {
                if (creationtime == sentence.creationtime) {
                    
                    // possibly a bit redundant, filters out duplicate titles 
                    // that have the same timestamp and same domain
                    // todo: this obviously does not correctly check for only
                    // titles with the same timestamp
                    if (titlesSeen.contains(sentence.content, sentence.domain))
                        return;
                } else {
                    creationtime = sentence.creationtime;
                }
                String dom = domain.getHostPart(sentence.domain);
                
                // possibly a bit redundant, if RemoveDuplicates was used 
                // strips non-content from titles
                sentence.content = TitleFilter.filterHost(dom, sentence.content);
                titlesSeen.add(sentence.content, sentence.domain);
                sentence.sentenceNumber = 0;
                sentence.write(sentenceFile);
            }
        } catch (Exception ex) {
            log.fatal("Exception %s %s", ex.getMessage(), sentenceFile.getDatafile().getCanonicalPath());
        }
    }

    @Override
    public void cleanup(Context context) {
        sentenceFile.closeWrite();
    }    
}
