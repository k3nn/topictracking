package stream5Retrieve.Unigram;

import static stream5Retrieve.RetrieveJob.*;
import io.github.k3nn.Node;
import io.github.htools.lib.Log;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Modifies the used known words model to unigrams
 * @author jeroen
 */
public class RetrieveMap extends stream5Retrieve.RetrieveMap {

    public static final Log log = new Log(RetrieveMap.class);

    public stream5Retrieve.RetrieveMap.Retriever getRetriever(Mapper.Context context, Setting settings) throws IOException {
        return new Retriever(context, settings);
    }

    static class Retriever<N extends Node> extends stream5Retrieve.RetrieveMap.Retriever<N> {

        protected Retriever(Mapper.Context context, Setting setting) throws IOException {
            super(context, setting);
        }

        protected KnownWords createKnownWords() {
            return new KnownWordsUnigram();
        }

    }
}
