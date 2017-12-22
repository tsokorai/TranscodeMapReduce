package MRTranscoder;

import io.humble.video.MediaPacket;

import io.humble.video.Muxer;

import io.humble.video.Rational;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class VideoRecordWriter extends RecordWriter<MediaStreamKeyWritable, MediaPacketWritable> {
    private static final Log LOG = LogFactory.getLog(VideoRecordWriter.class);
    Muxer out;
    int idx;
    Rational timebase;
    public VideoRecordWriter(Muxer out,int idx,Rational timebase) {
        this.out=out;
        this.idx=idx;
        this.timebase=timebase;
    }

    @Override
    public void write(MediaStreamKeyWritable key, MediaPacketWritable value) throws IOException, InterruptedException {
        LOG.warn("Writing k="+key+" v="+value);
        if(timebase!=null)
        {
            value.getPacket().setTimeBase(timebase);
        } 
        value.getPacket().setStreamIndex(idx);
        out.write(value.getPacket(), true);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // TODO Implement this method

    }
}
