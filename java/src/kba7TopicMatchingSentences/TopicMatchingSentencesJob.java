package kba7TopicMatchingSentences;

import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.io.IntLongWritable;
import io.github.htools.hadoop.Job;
import java.io.IOException;
import kbaeval.TopicFile;
import kbaeval.TopicWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import sentence.SentenceInputFormat;
import sentence.SentenceWritable;
import io.github.htools.collection.ArrayMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import static kba7TopicMatchingSentences.TopicMatchingSentencesMap.tokenizer;
import org.apache.hadoop.conf.Configuration;

/**
 * Create a list of all titles that contain all query terms. This list is used 
 * to flag "query matching clusters".
 * input: folder with YYYY-MM-DD SentenceFile that contain the titles of documents
 * that are used
 * output: folder that will contain a SentenceFile per topic in the testset with the
 * titles that contain all the topic's query terms
 * topicfile: TREC .xml file that contains the topics, using TopicFile reader
 * @author jeroen
 */
public class TopicMatchingSentencesJob {

    private static final Log log = new Log(TopicMatchingSentencesJob.class);
    private static long T_1 = 60 * 60 * 24 * 4;

    public static Job setup(String args[]) throws IOException {
        Conf conf = new Conf(args, "-i input -o output -t topicfile");
        conf.setReduceSpeculativeExecution(false);

        setTopics(conf);

        Job job = new Job(conf, conf.get("input"), conf.get("output"), conf.get("topicfile"));

        job.setInputFormatClass(SentenceInputFormat.class);
        SentenceInputFormat.addDirs(job, conf.get("input"));

        // fixed to 10, since there are 10 topics in TS2013
        job.setNumReduceTasks(10);
        job.setMapperClass(TopicMatchingSentencesMap.class);
        job.setReducerClass(TopicMatchingSentencesReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(IntLongWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);

        // not sure why we need to sort desc on creation time?
        job.setSortComparatorClass(IntLongWritable.DecreasingComparator.class);

        conf.getHDFSPath("output").trash();
        return job;
    }

    /**
     * Read topics from local FS, and store these in the Configuration
     * @param conf 
     */
    public static void setTopics(Conf conf) {
        TopicFile tf = new TopicFile(new Datafile(conf.get("topicfile")));
        for (TopicWritable topic : tf) {
            conf.addArray("topicquery", topic.query);
            conf.addArray("topicid", Integer.toString(topic.id));
            conf.addArray("topicstart", Long.toString(topic.start));
            conf.addArray("topicend", Long.toString(topic.end));
        }
    }

    /**
     * @param conf
     * @return list of topics read from Configuration
     */
    public static ArrayMap<TopicWritable, HashSet<String>> getTopics(Configuration conf) {
        ArrayMap<TopicWritable, HashSet<String>> result = new ArrayMap();
        String[] topics = conf.getStrings("topicquery");
        String[] ids = conf.getStrings("topicid");
        String[] starts = conf.getStrings("topicstart");
        String[] ends = conf.getStrings("topicend");
        for (int i = 0; i < topics.length; i++) {
            TopicWritable t = new TopicWritable();
            t.query = topics[i];
            t.id = Integer.parseInt(ids[i]);
            t.start = Long.parseLong(starts[i])  - T_1;
            t.end = Long.parseLong(ends[i]);
            HashSet<String> terms = new HashSet(tokenizer.tokenize(t.query));
            result.add(t, terms);
        }
        return result;
    }

    /**
     * @param topics
     * @return set containing query terms of all topics combined
     */
    public static HashSet<String> allTopicTerms(Iterable<HashSet<String>> topicterms) {
        HashSet<String> allterms = new HashSet();
        for (HashSet<String> set : topicterms) {
            allterms.addAll(set);
        }
        return allterms;
    }

    public static void main(String[] args) throws Exception {
        Job job = setup(args);
        job.waitForCompletion(true);
    }
}
