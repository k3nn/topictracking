package stream5Retrieve.NoTitle;

import static stream5Retrieve.RetrieveJob.*;
import matchingClusterNode.MatchingClusterNodeWritable;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.io.HDFSPath;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import stream5Retrieve.RetrieveReducer;

public class RetrieveJob {

    private static final Log log = new Log(RetrieveJob.class);

    public static Job setup(String args[]) throws IOException {
        Conf conf = new Conf(args, "-i input -o output -t topicfile");
        conf.setReduceSpeculativeExecution(false);
        conf.setMapSpeculativeExecution(false);
        conf.setMapMemoryMB(8192);
        conf.setReduceMemoryMB(4096);
        conf.setTaskTimeout(3600000);

        Job job = new Job(conf, conf.get("input"), conf.get("output"), conf.get("topicfile"));
        //job.getConfiguration().setInt("mapreduce.task.timeout", 1800000);

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

    public static void main(String[] args) throws Exception {
        Job job = setup(args);
        job.waitForCompletion(true);
    }
}
