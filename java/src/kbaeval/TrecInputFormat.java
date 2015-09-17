package kbaeval;

import io.github.htools.hadoop.io.StructuredFileInputFormat;

public class TrecInputFormat extends StructuredFileInputFormat<TrecFile, TrecWritable> {

    public TrecInputFormat() {
        super(TrecFile.class);
    }
}
