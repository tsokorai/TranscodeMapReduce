package MRTranscoder;

import java.io.IOException;

import io.humble.video.BitStreamFilter;
import io.humble.video.Coder;
import io.humble.video.ContainerFormat;
import io.humble.video.Decoder;
import io.humble.video.Demuxer;
import io.humble.video.DemuxerStream;
import io.humble.video.MediaDescriptor.Type;
import io.humble.video.MediaPacket;
import io.humble.video.Muxer;
import io.humble.video.MuxerFormat;

import io.humble.video.customio.URLProtocolManager;

import java.util.ArrayList;

import java.util.HashMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.ToolRunner;

public class Main {
    public Main() {
        super();
    }

// test method for direct HDFS transcode; this doesn't use MR at all, just to test HDFS access by the video libs
    public void transcode(String source, String dest, String vcodec) throws InterruptedException, IOException {
        // let's add our HDFS protocol handler, so that we can read from HDFS
        URLProtocolManager mgr = URLProtocolManager.getManager();
        mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory());
        final Demuxer demuxer = Demuxer.make();

        try {
            demuxer.open(source, null, false, true, null, null);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        final MuxerFormat format = MuxerFormat.guessFormat("matroska", null, null);
        final Muxer muxer = Muxer.make(dest, format, null);
        int n = demuxer.getNumStreams();
        System.out.println("Source file has " + n + " streams");
        HashMap<Integer, Decoder> decoders = new HashMap<Integer, Decoder>();
        for (int i = 0; i < n; i++) {
            final DemuxerStream ds = demuxer.getStream(i);
            final Decoder d = ds.getDecoder();

            if (d != null) {
                // neat; we can decode. Now let's see if this decoder can fit into the mp4 format.
                if (!format.getSupportedCodecs().contains(d.getCodecID())) {
                    System.out.println("Input filename (" + source +
                                       ") contains at least one stream with a codec not supported in the output format: " +
                                       d.toString());
                } else {
                    if (format.getFlag(MuxerFormat.Flag.GLOBAL_HEADER))
                        d.setFlag(Coder.Flag.FLAG_GLOBAL_HEADER, true);
                    d.open(null, null);
                    muxer.addNewStream(d);
                    System.out.println("Decoder:" + d.toString());
                    decoders.put(i, d);
                }
            }
        }
        muxer.open(null, null);
        final MediaPacket packet = MediaPacket.make();
        while (demuxer.read(packet) >= 0) {
            final Decoder d = decoders.get(packet.getStreamIndex());
            if (d != null) {
                if (packet.isComplete() && d != null) {
                    muxer.write(packet, true);
                }
            }
        }

        muxer.close();
        demuxer.close();
    }

    public static void main(String[] args) {
        Main main = new Main();
        Configuration conf = new Configuration();
        int res = -1;
        // Now here's the not-very-obvious-part: for -libjars to work, your MR program has to use GenericOptionsParser or
        // run through ToolRunner, otherwise it doesn't do diddly-squat 
        try {
            res = ToolRunner.run(conf, new TranscoderTool(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(res);
        /* try {
            main.transcode(args[0], args[1], args[2]);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } */
    }
}
