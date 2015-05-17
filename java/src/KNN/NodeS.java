package KNN;

import io.github.repir.tools.lib.Log;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import java.util.Collection;
import java.util.HashSet;

/**
 * Extension of NodeT that also holds an array of tokenized terms.
 * @author jeroen
 */
public class NodeS extends NodeT {

    static Log log = new Log(NodeS.class);
    HashSet<String> terms = new HashSet();

    public NodeS(long id, int domain, String title, Collection<String> features, long creationtime) {
        super(id, domain, title, features, creationtime);
        setFeatures(features);
    }

    public HashSet<String> getTerms() {
        return terms;
    }
    
    public void setFeatures(Collection<String> features) {
        this.terms.addAll(features);
        this.featureCount = this.terms.size();
    }

    @Override
    public String toString() {
        if (1==1) {
            return sprintf("Url %d", getID());
        }
        StringBuilder sb = new StringBuilder();
        //log.info("toString %d %d %s", getID(), edges, features);
        sb.append(sprintf("Url [%d] %s", edges, terms));
        //for (int i = 0; i < edges; i++) {
        //    sb.append(nn[i].toString());
        //}
        return sb.toString();
    }

    public String toStringEdges() {
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("Url [%d] %s", edges, terms));
        for (int i = 0; i < edges; i++) {
            sb.append("\n").append(edge[i].toString());
        }
        return sb.toString();
    }
}