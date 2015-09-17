package createIDF;

import io.github.k3nn.ClusteringGraph;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import io.github.htools.collection.HashMapInt;
import io.github.htools.extract.Content;
import io.github.htools.extract.WikipediaSourceSplitter;
import io.github.htools.extract.WikipediaSourceSplitter.Result;
import io.github.htools.search.ByteSearch;
import io.github.htools.search.ByteSearchPosition;
import org.apache.hadoop.io.Text;

/**
 * counts the document frequency of terms occurring in Wikipedia pages 
 * (MediaWiki format).
 *
 * @author jeroen
 */
public class CreateIDFMap extends Mapper<LongWritable, Content, Text, IntWritable> {

    public static final Log log = new Log(CreateIDFMap.class);
    // tokenizes on non-alphanumeric characters, lowercase, stop words removed, no stemming
    WikipediaSourceSplitter wpsplitter = new WikipediaSourceSplitter();
    DefaultTokenizer tokenizer = ClusteringGraph.getUnstemmedTokenizer();
    HashMapInt<String> termDocumentFrequency;
    ByteSearch bar = ByteSearch.create("\\|");
    ByteSearch block = ByteSearch.create("[\\[\\]]");

    @Override
    public void setup(Context context) throws IOException {
        termDocumentFrequency = new HashMapInt();
    }

    @Override
    public void map(LongWritable key, Content value, Context context) {
        
        // parse wikipedia page into .text, .table and .macro, we only use
        // .text and .macro.
        Result content = wpsplitter.tokenize(value.content);
        
        // for document frequency we only count unique terms
        HashSet<String> terms = new HashSet();
        for (byte[] text : content.text) {
            ArrayList<String> tokenize = tokenizer.tokenize(text);
            terms.addAll(tokenize);
        }
        
        for (byte[] text : content.macro) {
            
            // remove all text before the last | in a mediaWiki macro
            // to convert [New York|The Big Apple] to the displayed text "the big apple" 
            ByteSearchPosition pos = bar.findLastPos(text, 0, text.length);
            if (pos.found()) {
                for (int i = 0; i < pos.end; i++)
                    text[i] = 0;
            }
            // replace [] by empty string, so that [hurricane]s is converted
            // to hurricanes
            block.replaceAll(text, 0, text.length, "");
            
            ArrayList<String> tokenize = tokenizer.tokenize(text);
            terms.addAll(tokenize);
        }
        // # is used a token to count the total number of documents
        termDocumentFrequency.add("#");
        termDocumentFrequency.addAll(terms);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        Text term = new Text();
        IntWritable documentFrequency = new IntWritable();
        for (Map.Entry<String, Integer> entry : termDocumentFrequency.entrySet()) {
            term.set(entry.getKey());
            documentFrequency.set(entry.getValue());
            context.write(term, documentFrequency);
        }
    }
}
