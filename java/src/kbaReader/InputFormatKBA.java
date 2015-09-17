package kbaReader;

import io.github.htools.hadoop.InputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import kbaThriftReader.StreamItem;

/**
 * Hadoop reader for KBA Streaming corpus, un-gpg-ed and xz compressed
 * @author jeroen
 */
public class InputFormatKBA extends InputFormat<StreamItem> {

    @Override
    public RecordReader<LongWritable, StreamItem> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        return new RecordReaderKBA();
    }

}
