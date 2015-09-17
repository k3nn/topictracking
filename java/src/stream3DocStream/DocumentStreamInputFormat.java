package stream3DocStream;

import io.github.htools.hadoop.io.StructuredFileInputFormat;

public class DocumentStreamInputFormat extends StructuredFileInputFormat<DocumentStreamFile, DocumentStreamWritable> {

    public DocumentStreamInputFormat() {
        super(DocumentStreamFile.class);
    }
}
