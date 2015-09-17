package kbaeval;

import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
/**
 * List the topics in a TREC XML TopicFile
 * @author jeroen
 */
public class ListTopics {
   public static final Log log = new Log( ListTopics.class );

    public static void main(String[] args) {
        TopicFile tf = new TopicFile(new Datafile(args[0]));
        for (TopicWritable t : tf) {
            log.info("%d %s", t.id, t.query);
        }
    }
}
