package MRTranscoder;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator() {
        super(MediaStreamKeyWritable.class,true);
    }
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        MediaStreamKeyWritable k1 = (MediaStreamKeyWritable)w1;
        MediaStreamKeyWritable k2 = (MediaStreamKeyWritable)w2;
        int result = k1.getPos().compareTo(k2.getPos());
        if(0 == result) {
            result = -1* k1.getStream().compareTo(k2.getStream());
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }
}
