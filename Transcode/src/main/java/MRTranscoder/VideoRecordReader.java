package MRTranscoder;

import io.humble.video.Coder;
import io.humble.video.Container;
import io.humble.video.ContainerFormat;
import io.humble.video.Decoder;
import io.humble.video.Demuxer;
import io.humble.video.DemuxerStream;

import io.humble.video.MediaDescriptor;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

import io.humble.video.MediaPacket;

import io.humble.video.Muxer;
import io.humble.video.MuxerFormat;
import io.humble.video.customio.URLProtocolManager;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class VideoRecordReader extends RecordReader<MediaStreamKeyWritable, MediaPacketWritable> {
    private static final Log LOG = LogFactory.getLog(VideoRecordReader.class);
    private long start;
    private long pos;
    private long end,len;
    private float progress;
    private Path splitFilePath = null;
    FSDataInputStream filein = null;
    Demuxer demuxer = null;
    int numStreams = 0;
    HashMap<Integer, Decoder> decoders = new HashMap<Integer, Decoder>();
    long currentKey=0;
    int currentStream=0;
    MediaPacketWritable currentValue=null;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException,
                                                                                                InterruptedException {
        // let's add our HDFS protocol handler, so that we can read from HDFS
        URLProtocolManager mgr = URLProtocolManager.getManager();
        mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory());
        FileSplit split = (FileSplit) inputSplit;
        start = split.getStart();
        len=split.getLength();
        end = start + len;
        Configuration conf = taskAttemptContext.getConfiguration();
        splitFilePath = split.getPath();
        LOG.warn("Split file: " + splitFilePath.toString() + " pos=" + start + " len=" + split.getLength());
        demuxer = Demuxer.make();
        demuxer.setFlag(Container.Flag.FLAG_CUSTOM_IO, true);
        demuxer.setFlag(Container.Flag.FLAG_DISCARD_CORRUPT, true);
       demuxer.setFlag(Container.Flag.FLAG_NOBUFFER, true);
  //      demuxer.setFlag(Container.Flag.FLAG_NOFILLIN, true);
//        demuxer.setFlag(Container.Flag.FLAG_NOPARSE, true);
        try {
            demuxer.open(splitFilePath.toString(), null, false, true, null, null);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        final MuxerFormat format = MuxerFormat.guessFormat("matroska", null, null);
        numStreams = demuxer.getNumStreams();
        LOG.info("Source file has " + numStreams + " streams");
        int streamsToBeHandled=0;
        for (int i = 0; i < numStreams; i++) {
            final DemuxerStream ds = demuxer.getStream(i);
            final Decoder d = ds.getDecoder();

            if (d != null) {
                // neat; we can decode. Now let's see if this decoder can fit into the mp4 format.
                if (!format.getSupportedCodecs().contains(d.getCodecID())) {
                    LOG.warn("Input filename (" + splitFilePath.toString() +
                                       ") contains at least one stream with a codec not supported in the output format: " +
                                       d.toString());
                } else {
                    d.open(null, null);
                    LOG.info("Decoder:" + d.toString());
                    decoders.put(i, d);
                    demuxer.seek(i,start,start,start,Demuxer.SeekFlag.SEEK_BYTE.swigValue());
                }
            }
        }
        progress=0.0f;
        currentValue=new MediaPacketWritable();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        int ret=-1;
        boolean haveKey=false;
        if(pos<=end)
        {
            // we need to make sure to read a whole packet, since it can take several reads...
            try {
                while ((ret=demuxer.read(currentValue.getPacket()))>=0) {
                    final Decoder d = decoders.get(currentValue.getPacket().getStreamIndex());
                    if (currentValue.getPacket().isComplete() && (d != null)) {
                        currentStream=currentValue.getPacket().getStreamIndex();
                        haveKey=true;
                        break;
                    }
                }
            } catch (Exception e) {
                LOG.info("pos="+pos+" start="+start+" end="+end+ " prog="+progress+ " ret="+ret);
                LOG.warn("Got exception while getting next keyValue",e);
            }
            if(haveKey)
            {
                long p=currentValue.getPacket().getPosition();
                
                if(p>=0)
                {
                    pos=p;
                    currentKey=pos;
                    progress=(float)(pos-start)/(float)len;
                }
            }
        }
        
        return (haveKey);
    }

    @Override
    public MediaStreamKeyWritable getCurrentKey() throws IOException, InterruptedException {
        return new MediaStreamKeyWritable(currentStream,currentKey);
    }

    @Override
    public MediaPacketWritable getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return progress;
    }

    @Override
    public void close() throws IOException {
        /* try {
            demuxer.close();
        } catch (InterruptedException e) {
            LOG.error("Error closing demuxer",e);
        } */
    }
}
