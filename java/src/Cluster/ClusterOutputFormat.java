package Cluster;

import io.github.repir.tools.hadoop.Structured.OutputFormat;
/**
 *
 * @author jeroen
 */
public class ClusterOutputFormat extends OutputFormat<ClusterFile, ClusterWritable> {

    public ClusterOutputFormat() {
        super(ClusterFile.class, ClusterWritable.class);
    }

}