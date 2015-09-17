package stream3DocStream;

import io.github.htools.io.Datafile;
import io.github.htools.hadoop.tsv.File;

/**
 *
 * @author jeroen
 */
public class DocumentStreamFile extends File<DocumentStreamWritable> {

    public StringField docid = this.addString("docid");
    public LongField creationtime = this.addLong("creationtime");
    public BoolField isCandidate = this.addBoolean("iscandidate");

    public DocumentStreamFile(Datafile datafile) {
        super(datafile);
    }

    @Override
    public DocumentStreamWritable newRecord() {
        return new DocumentStreamWritable();
    }  
}
