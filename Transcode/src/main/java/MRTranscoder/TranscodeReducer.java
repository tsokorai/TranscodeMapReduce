package MRTranscoder;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TranscodeReducer extends Reducer<MediaStreamKeyWritable, MediaPacketWritable, MediaStreamKeyWritable, MediaPacketWritable>  {
    public TranscodeReducer() {
        super();
    }

    @Override
    protected void reduce(MediaStreamKeyWritable mediaStreamKeyWritable, Iterable<MediaPacketWritable> iterable,
                          Reducer<MediaStreamKeyWritable, MediaPacketWritable, MediaStreamKeyWritable, MediaPacketWritable>.Context context) throws IOException,
                                                                                                                                                    InterruptedException {

        for (MediaPacketWritable val : iterable) {
                 context.write(mediaStreamKeyWritable, val);
             }
        
    }
}
