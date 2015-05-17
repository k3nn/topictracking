package MatchingClusterNode;

import io.github.repir.tools.hadoop.io.OutputFormat;
/**
 *
 * @author jeroen
 */
public class MatchingClusterNodeOutputFormat extends OutputFormat<MatchingClusterNodeFile, MatchingClusterNodeWritable> {

    public MatchingClusterNodeOutputFormat() {
        super(MatchingClusterNodeFile.class, MatchingClusterNodeWritable.class);
    }

}