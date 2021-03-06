package kbaeval;

import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.hadoop.tsv.Writable;
/**
 *
 * @author jeroen
 */
public class MatchEditWritable extends Writable<MatchEditFile> {
    public int query_id;
    public String update_id;
    public String nugget_id;
    public String match;

    @Override
    public void read(MatchEditFile f) {
        this.query_id = f.query_id.get();
        this.update_id = f.update_id.get();
        this.nugget_id = f.nugget_id.get();
        this.match = f.match.get();
    }

    @Override
    public void write(BufferDelayedWriter writer)  {
        writer.write(query_id);
        writer.write(update_id);
        writer.write(nugget_id);
        writer.write(match);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
       query_id = reader.readInt();
       update_id = reader.readString();
       nugget_id = reader.readString();
       match = reader.readString();
    }

    @Override
    public void write(MatchEditFile file) {
        file.query_id.set(query_id);
        file.update_id.set(update_id);
        file.nugget_id.set(nugget_id);
        file.match.set(match);
        file.write();
    }
}
