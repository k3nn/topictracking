package stream1ClusterTitles;

import cluster.ClusterFile;
import cluster.ClusterWritable;
import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.hadoop.io.IntIntWritable;
import io.github.htools.hadoop.io.IntLongIntWritable;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Write the cluster snapshots to a file per topic.
 * @author jeroen
 */
public class ClusterTitlesReducer extends Reducer<IntLongIntWritable, ClusterWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(ClusterTitlesReducer.class);
    ClusterFile clusterFile;

    @Override
    public void setup(Context context) throws IOException {
        clusterFile = getClusterFile(context);
    }

    @Override
    public void reduce(IntLongIntWritable key, Iterable<ClusterWritable> values, Context context) throws IOException, InterruptedException {
        for (ClusterWritable value : values) {
            value.write(clusterFile);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        clusterFile.closeWrite();
    }
    
    /**
     * returns a ClusterFile with the same name as the topMatchingSentenceFile
     * that was used in the input, using the reducerID as index.
     */
    public ClusterFile getClusterFile(Context context) throws IOException {
        Conf conf = ContextTools.getConfiguration(context);
        ArrayList<Datafile> topicFiles = ClusterTitlesJob.getTopicMatchingSentenceFiles(conf);
        int reducer = ContextTools.getTaskID(context);
        String topicfilename = topicFiles.get(reducer).getName();
        
        HDFSPath outputPath = conf.getHDFSPath("output");
        Datafile datafile = outputPath.getFile(topicfilename);
        return new ClusterFile(datafile);       
    }
}
