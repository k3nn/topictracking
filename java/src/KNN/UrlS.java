package KNN;

import io.github.repir.tools.Lib.Log;
import static io.github.repir.tools.Lib.PrintTools.sprintf;
import io.github.repir.tools.Lib.UrlStrTools;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

/**
 *
 * @author jeroen
 */
public class UrlS extends UrlT {

    static Log log = new Log(UrlS.class);
    HashSet<String> features = new HashSet();

    public UrlS(int id, int domain, String title, Collection<String> features, long creationtime) {
        super(id, domain, title, features, creationtime);
        this.setFeatures(features);
    }

    public HashSet<String> getFeatures() {
        return features;
    }
    
    public void setFeatures(Collection<String> features) {
        this.features.addAll(features);
        this.featureCount = this.features.size();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        log.info("toString %d %d %s", getID(), edges, features);
        sb.append(sprintf("Url [%d] %s", edges, features));
        //for (int i = 0; i < edges; i++) {
        //    sb.append(nn[i].toString());
        //}
        return sb.toString();
    }

    public String toStringEdges() {
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("Url [%d] %s", edges, features));
        for (int i = 0; i < edges; i++) {
            sb.append("\n").append(edge[i].toString());
        }
        return sb.toString();
    }
}