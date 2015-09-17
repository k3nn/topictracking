package kbaReader;

import io.github.htools.hadoop.InputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import kbaThriftReader.StreamItem;

/**
 * Reader for KBA streaming corpus, version that was repacked into Hadoop sequence files and gzipped
 * @author jeroen
 */
public class InputFormatKBAGZ extends InputFormat<StreamItem> {

    @Override
    public RecordReader<LongWritable, StreamItem> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        return new RecordReaderKBAGZ();
    }

}
