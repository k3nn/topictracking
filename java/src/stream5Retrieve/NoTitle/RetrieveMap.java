package stream5Retrieve.NoTitle;

import static stream5Retrieve.RetrieveJob.*;
import io.github.k3nn.Node;
import io.github.k3nn.impl.NodeSentence;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.HashSet;

/**
 * Adds requirement that sentences with number 0 (i.e. titles) never qualify
 * @author jeroen
 */
public class RetrieveMap extends stream5Retrieve.RetrieveMap {

    public static final Log log = new Log(RetrieveMap.class);

    public stream5Retrieve.RetrieveMap.Retriever getRetriever(Context context, Setting settings) throws IOException {
        return new Retriever(context, settings);
    }

    static class Retriever<N extends Node> extends stream5Retrieve.RetrieveMap.Retriever<N> {

        protected Retriever(Context context, Setting setting) throws IOException {
            super(context, setting);
        }

        // do not use titles
        @Override
        public boolean withinTopicInterval(N candidateNode) {
            return ((NodeSentence) candidateNode).sentence != 0 && 
                    super.withinTopicInterval(candidateNode);
        }

    }
}
