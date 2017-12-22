package MRTranscoder;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import io.humble.video.MediaPacket;

import org.apache.hadoop.io.LongWritable;

public class VideoInputFileFormat extends FileInputFormat<MediaStreamKeyWritable,MediaPacketWritable> {
    public VideoInputFileFormat() {
        super();
    }

    @Override
    public RecordReader<MediaStreamKeyWritable,MediaPacketWritable> createRecordReader(InputSplit inputSplit,
                                           TaskAttemptContext taskAttemptContext) throws IOException,
                                                                                         InterruptedException {
        return new VideoRecordReader();
    }
}
