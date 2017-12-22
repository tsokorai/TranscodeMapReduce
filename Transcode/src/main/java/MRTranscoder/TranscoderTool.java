package MRTranscoder;

import io.humble.video.Container;
import io.humble.video.ContainerFormat;
import io.humble.video.Decoder;
import io.humble.video.Demuxer;
import io.humble.video.DemuxerStream;
import io.humble.video.MediaDescriptor;
import io.humble.video.MediaPacket;

import io.humble.video.MuxerFormat;
import io.humble.video.customio.URLProtocolManager;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;

public class TranscoderTool extends Configured implements Tool {
    public TranscoderTool(Configuration configuration) {
        super(configuration);
    }

    public TranscoderTool() {
        super();
    }

// this puts the data so that all mappers/reducers can access the stream params 
    private void putStreamDataIntoConfig(Configuration conf,String path){
        // let's add our HDFS protocol handler, so that we can read from HDFS
        URLProtocolManager mgr = URLProtocolManager.getManager();
        mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory());
        Demuxer demuxer = Demuxer.make();
        demuxer.setFlag(Container.Flag.FLAG_CUSTOM_IO, true);
        demuxer.setFlag(Container.Flag.FLAG_DISCARD_CORRUPT, true);
        demuxer.setFlag(Container.Flag.FLAG_NOBUFFER, true);
        //      demuxer.setFlag(Container.Flag.FLAG_NOFILLIN, true);
        //        demuxer.setFlag(Container.Flag.FLAG_NOPARSE, true);
        try {
            demuxer.open(path, null, false, true, null, null);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        final MuxerFormat format = MuxerFormat.guessFormat("matroska", null, null);
        int numStreams=0;
        try {
            numStreams = demuxer.getNumStreams();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        conf.setInt("InputVideoNumStreams",numStreams);
        int streamsToBeHandled=0;
        for (int i = 0; i < numStreams; i++) {
            DemuxerStream ds=null;
            try {
                ds = demuxer.getStream(i);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
            final Decoder d = ds.getDecoder();

            if (d != null) {
                // neat; we can decode. Now let's see if this decoder can fit into the mp4 format.
                if (format.getSupportedCodecs().contains(d.getCodecID())) {
                    if (format.getFlag(MuxerFormat.Flag.GLOBAL_HEADER))
                        conf.setBoolean("mediaStreamCodecGlobalHeader_"+i,format.getFlag(MuxerFormat.Flag.GLOBAL_HEADER));
                   
                    // let's save the media info for the streams so that the mapper and reducer can use it. 
                    
                    conf.setInt("mediaStreamCodecType_"+i,d.getCodecType().swigValue());
                    conf.setInt("mediaStreamCodecID_"+i,d.getCodecID().swigValue());
                    
                    conf.setInt("mediaStreamCodecTimeBaseDenom_"+i,d.getTimeBase().getDenominator());
                    conf.setInt("mediaStreamCodecTimeBaseNum_"+i,d.getTimeBase().getNumerator());
                    if(d.getCodecType()==MediaDescriptor.Type.MEDIA_VIDEO) {
                        conf.setInt("mediaStreamCodecWidth_"+i,d.getWidth());
                        conf.setInt("mediaStreamCodecHeight_"+i,d.getHeight());
                        conf.setInt("mediaStreamCodecPixelFormat_"+i,d.getPixelFormat().swigValue());
                    } else if(d.getCodecType()==MediaDescriptor.Type.MEDIA_AUDIO) {
                        conf.setInt("mediaStreamCodecSampleRate_"+i,d.getSampleRate());
                        conf.setInt("mediaStreamCodecChannels_"+i,d.getChannels());
                        conf.setInt("mediaStreamCodecSampleFormat_"+i,d.getSampleFormat().swigValue());
                        conf.setInt("mediaStreamCodecChannelLayout_"+i,d.getChannelLayout().swigValue());
                    }
                    streamsToBeHandled++;

                }
            }
            conf.setInt("mediaStreamsToBeHandled",streamsToBeHandled);
        }
    }

    @Override
    public int run(String[] string) throws Exception {
        Configuration conf = getConf();
        conf.set("destinationCodec", string[2]);
        putStreamDataIntoConfig(conf, string[0]);
        Job job=null;
        try {
            job = Job.getInstance(conf, "HWXCLTranscode");
        } catch (IOException e) {
            e.printStackTrace();
        }
        job.setJarByClass(TranscoderTool.class);
            job.setMapperClass(TranscodeMapper.class);
            //job.setCombinerClass(IntSumReducer.class);
            job.setSortComparatorClass(CompositeKeyComparator.class);
            job.setReducerClass(TranscodeReducer.class);
            job.setOutputKeyClass(MediaStreamKeyWritable.class);
            job.setOutputValueClass(MediaPacketWritable.class);
            // my own reader classes to handle video files
            job.setOutputFormatClass(VideoOutputFileFormat.class);
            job.setInputFormatClass(VideoInputFileFormat.class);
        try {
            VideoInputFileFormat.addInputPath(job, new Path(string[0]));
        } catch (IOException e) {
            e.printStackTrace();
        }
        VideoOutputFileFormat.setOutputPath(job, new Path(string[1]));
        try {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (ClassNotFoundException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
