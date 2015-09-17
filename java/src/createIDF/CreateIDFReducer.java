package createIDF;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Aggregates the term document frequencies and writes to an IDF file. "#" is
 * a token used for the total number of documents in the corpus.
 * @author jeroen
 */
public class CreateIDFReducer extends Reducer<Text, IntWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(CreateIDFReducer.class);
    Datafile df;

    @Override
    public void setup(Context context) throws IOException {
        Conf conf = ContextTools.getConfiguration(context);
        df = conf.getHDFSFile("output");
        df.openWrite();
    }

    @Override
    public void reduce(Text term, Iterable<IntWritable> documentFrequencies, Context context) throws IOException, InterruptedException {
        int documentFrequency = 0;
        for (IntWritable frequency : documentFrequencies) {
            documentFrequency += frequency.get();
        }
        // only output the terms with df > 9
        if (documentFrequency > 9)
           df.printf("%s\t%d\n", term.toString(), documentFrequency);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        df.closeWrite();
    }
}
