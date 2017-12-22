package MRTranscoder;

import io.humble.video.Codec;
import io.humble.video.Coder;
import io.humble.video.Encoder;
import io.humble.video.MediaDescriptor;
import io.humble.video.MediaPacket;

import io.humble.video.Muxer;
import io.humble.video.MuxerFormat;
import io.humble.video.MuxerStream;
import io.humble.video.PixelFormat;
import io.humble.video.Rational;
import io.humble.video.customio.URLProtocolManager;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class VideoOutputFileFormat extends FileOutputFormat<MediaStreamKeyWritable, MediaPacketWritable> {
    private static final Log LOG = LogFactory.getLog(VideoOutputFileFormat.class);
    public VideoOutputFileFormat() {
        super();
    }

    @Override
    public VideoRecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException,InterruptedException {
        Path file = getOutputPath(taskAttemptContext);
        LOG.info("Saving to "+file);
        int numStreams=taskAttemptContext.getConfiguration().getInt("mediaStreamsToBeHandled", 0);
        int videoStream=0;
        for (int i = 0; i < numStreams; i++) {
            if(taskAttemptContext.getConfiguration().getInt("mediaStreamCodecType_" + i, 0) ==
                    MediaDescriptor.Type.MEDIA_VIDEO.swigValue()) {
                videoStream=i;
                LOG.info("Video stream is stream = "+videoStream);
                break;
            }
        }
        URLProtocolManager mgr = URLProtocolManager.getManager();
        mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory());
        // currently hardwired to mkv
        MuxerFormat muxFormat = MuxerFormat.guessFormat("matroska", null, null);
        // since MR frwk seems to be hellbent on creating a directory for our destination, then we just use a fixed
        // filename for our file, inside the destination dir
        String finalFile=file.toString()+"/destination.mkv";
        Muxer muxer = Muxer.make(finalFile, muxFormat, null);
        Codec codec = Codec.findEncodingCodecByName(taskAttemptContext.getConfiguration().get("destinationCodec"));
        // DANGER WILL ROBINSON... the following are hardwired to stream 0, sorry, no time to do proper stream mapping
        PixelFormat.Type pixelFormat =
        PixelFormat.Type.swigToEnum(taskAttemptContext.getConfiguration()
                                    .getInt("mediaStreamCodecPixelFormat_"+videoStream, 0));
        Encoder videoEncoder = null;
        videoEncoder = Encoder.make(codec);
        videoEncoder.setPixelFormat(pixelFormat);
        videoEncoder.setWidth(640);
        videoEncoder.setHeight(480);
        Rational tb =
            Rational.make(taskAttemptContext.getConfiguration().getInt("mediaStreamCodecTimeBaseNum_"+videoStream , 0),
                          taskAttemptContext.getConfiguration().getInt("mediaStreamCodecTimeBaseDenom_"+videoStream , 0));
        if(tb!=null)
        {
            videoEncoder.setTimeBase(tb);
            LOG.info("timebase den="+tb.getDenominator()+ " num="+tb.getNumerator());
        } else {
            LOG.warn("TIMEBASE NULL");
        }
       
        videoEncoder.setFlag(Coder.Flag.FLAG_GLOBAL_HEADER, true);
        videoEncoder.open(null, null);
        MuxerStream strm=muxer.addNewStream(videoEncoder);
        int idx=strm.getIndex();
        LOG.info("stream idx for new stream="+idx);
        muxer.open(null, null);
        return new VideoRecordWriter(muxer,idx,tb);
    }
}
