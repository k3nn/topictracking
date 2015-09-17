package stream5Retrieve;

import matchingClusterNode.MatchingClusterNodeWritable;
import cluster.ClusterFile;
import cluster.ClusterWritable;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.ParamFileInputFormat;
import io.github.htools.io.EOCException;
import io.github.htools.io.HDFSPath;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.io.buffer.BufferSerializable;
import io.github.htools.io.struct.StructureReader;
import io.github.htools.io.struct.StructureWriter;
import io.github.htools.lib.MathTools;
import io.github.htools.lib.StrTools;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import static kbaeval.TopicFile.getTopics;
import kbaeval.TopicWritable;
import kbaeval.TrecWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * Constructs a summary based on the per topic sentence clustering (stream4) by
 * qualifying sentences against a relevance model of the last h hours, using
 * only sentences of at most l non stop words, that have at least a gain of g%
 * novel information (measured either by unigrams or 2-word combinations) and
 * rank in the top-r compared to previously selected sentences.
 * <p/>
 * This retriever performs a sweep over a range of parameter settings for h, l,
 * g, and r and stores every result in a different file.
 *
 * @author Jeroen
 */
public class RetrieveJob {

    private static final Log log = new Log(RetrieveJob.class);
    static Conf conf;

    public static Job setup(String args[]) throws IOException {
        conf = new Conf(args, "-i input -o output -t topicfile");
        conf.setReduceStart(0.5);
        conf.setReduceSpeculativeExecution(false);
        conf.setMapSpeculativeExecution(false);
        conf.setMapMemoryMB(8192);
        conf.setReduceMemoryMB(4096);
        conf.setTaskTimeout(3600000);

        Job job = new Job(conf,
                conf.get("input"),
                conf.get("output"),
                conf.get("topicfile"));

        job.setInputFormatClass(InputFormat.class);
        addInput(job, conf.getHDFSPath("input"), conf.get("topicfile"));

        job.setNumReduceTasks(getParams().size());
        job.setMapperClass(RetrieveMap.class);
        job.setReducerClass(RetrieveReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(Setting.class);
        job.setMapOutputValueClass(MatchingClusterNodeWritable.class);
        job.setPartitionerClass(SettingPartitioner.class);

        return job;
    }

    public static void addInput(Job job, HDFSPath sentenceClustersPerTopicPath, String topicfile) throws IOException {
        HashMap<Integer, TopicWritable> topics = getTopics(topicfile);
        for (Datafile datafile : sentenceClustersPerTopicPath.getFiles()) {
            String topicext = StrTools.getAfterLastString(datafile.getName(), ".");
            if (topicext.length() > 0) {
                int topicid = Integer.parseInt(topicext);
                TopicWritable topic = topics.get(topicid);
                Collection<Setting> params = getParams();
                for (Setting setting : params) {
                    setting.topicid = topic.id;
                    setting.topicstart = topic.start;
                    setting.topicend = topic.end;
                    setting.query = topic.query;
                    InputFormat.add(job, setting, new Path(datafile.getCanonicalPath()));
                }
            }
        }
    }

    public static class InputFormat extends ParamFileInputFormat<ClusterFile, Setting, ClusterWritable> {

        public InputFormat() {
            super(ClusterFile.class);
        }

        @Override
        protected Setting createKey() {
            return new Setting();
        }

    }

    public static class Setting implements WritableComparable<Setting>, BufferSerializable {
        // default settings
        public double gainratio = 0.3;
        public double hours = 1.0;
        public int length = 20;
        public int topk = 5;
        public String query;
        public int topicid;
        public long topicstart;
        public long topicend;

        @Override
        public int hashCode() {
            return MathTools.hashCode(length, topk, (int) (gainratio * 100), (int) (hours * 10));
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Setting) {
                Setting s = (Setting) o;
                return gainratio == s.gainratio && hours == s.hours && length == s.length && topk == s.topk;
            }
            return false;
        }

        @Override
        public void read(StructureReader reader) throws EOCException {
            gainratio = reader.readDouble();
            hours = reader.readDouble();
            length = reader.readInt();
            topk = reader.readInt();
            topicid = reader.readInt();
            topicstart = reader.readLong();
            topicend = reader.readLong();
            query = reader.readString();
        }

        @Override
        public void write(StructureWriter writer) {
            writer.write(gainratio);
            writer.write(hours);
            writer.write(length);
            writer.write(topk);
            writer.write(topicid);
            writer.write(topicstart);
            writer.write(topicend);
            writer.write(query);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            BufferDelayedWriter writer = new BufferDelayedWriter();
            write(writer);
            writer.writeBuffer(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            BufferReaderWriter reader = new BufferReaderWriter(dataInput);
            this.read(reader);
        }

        @Override
        public int compareTo(Setting otherSetting) {
            return topicid - otherSetting.topicid;
        }
    }

    public static class SettingPartitioner extends Partitioner<Setting, Object> {

        HashMap<Setting, Integer> params = new HashMap();

        public SettingPartitioner() {
            for (Setting setting : getParams()) {
                params.put(setting, params.size());
            }
        }

        @Override
        public int getPartition(Setting setting, Object value, int i) {
            return params.get(setting);
        }
    }

    /**
     * @return a list that contains a sweep of parameter settings 
     */
    public static ArrayList<Setting> getParams() {
        ArrayList<Setting> settings = getParamsS();
        for (double gainratio : new double[]{0.1, 0.15, 0.2, 0.25, 0.35, 0.4, 0.45, 0.50, 0.55, 0.6, 0.65, 0.7}) {
            Setting setting = new Setting();
            setting.gainratio = gainratio;
            settings.add(setting);
        }
        for (double hours : new double[]{0.5, 2, 3}) {
            Setting setting = new Setting();
            setting.hours = hours;
            settings.add(setting);
        }
        for (int length : new int[]{11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 23, 24, 25, 26, 27, 28, 29}) {
            Setting setting = new Setting();
            setting.length = length;
            settings.add(setting);
        }
        for (int topk : new int[]{1, 2, 3, 4, 6, 7, 8, 9, 10}) {
            Setting setting = new Setting();
            setting.topk = topk;
            settings.add(setting);
        }
        return settings;
    }

    public static ArrayList<Setting> getParamsS() {
        ArrayList<Setting> settings = new ArrayList();
        settings.add(new Setting());
        return settings;
    }

    public static void main(String[] args) throws Exception {
        Job job = setup(args);
        job.waitForCompletion(true);
    }
}
