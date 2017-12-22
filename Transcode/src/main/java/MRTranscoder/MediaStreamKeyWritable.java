package MRTranscoder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class MediaStreamKeyWritable implements Writable, WritableComparable {
    private static final Log LOG = LogFactory.getLog(MediaStreamKeyWritable.class);
    @Override
    public String toString() {
        return "<pos="+pos+",stream="+stream+">";
    }
    private int stream;
    private long pos;
    
    public MediaStreamKeyWritable() {
        this.pos=0;
        this.stream=0;
    }
    
    public MediaStreamKeyWritable(int stream,long pos) {
        this.pos=pos;
        this.stream=stream;
    }
    
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(stream);
        dataOutput.writeLong(pos);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        stream=dataInput.readInt();
        pos=dataInput.readLong();
    }

    public void setStream(int stream) {
        this.stream = stream;
    }

    public Integer getStream() {
        return stream;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public Long getPos() {
        return pos;
    }

    @Override
    public int compareTo(Object o) {
        int result=-1;
        if(o instanceof MediaStreamKeyWritable){
            MediaStreamKeyWritable k1 = (MediaStreamKeyWritable)o;
            result = this.getPos().compareTo(k1.getPos());
            if(0 == result) {
                result = -1* this.getStream().compareTo(k1.getStream());
            }
        }
       
        return result;
    }
}
