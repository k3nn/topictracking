package stream3DocStream;

import io.github.htools.hadoop.io.OutputFormat;
/**
 *
 * @author jeroen
 */
public class DocumentStreamOutputFormat extends OutputFormat<DocumentStreamFile, DocumentStreamWritable> {

    public DocumentStreamOutputFormat() {
        super(DocumentStreamFile.class, DocumentStreamWritable.class);
    }

}
