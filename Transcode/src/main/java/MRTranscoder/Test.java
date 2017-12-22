package MRTranscoder;

import io.humble.video.AudioChannel;
import io.humble.video.AudioFormat;
import io.humble.video.Codec;
import io.humble.video.Coder;
import io.humble.video.Container;
import io.humble.video.ContainerFormat;
import io.humble.video.Decoder;
import io.humble.video.Demuxer;
import io.humble.video.DemuxerFormat;
import io.humble.video.DemuxerStream;
import io.humble.video.Encoder;
import io.humble.video.MediaPacket;
import io.humble.video.Muxer;
import io.humble.video.MuxerFormat;
import io.humble.video.PixelFormat;
import io.humble.video.Rational;
import io.humble.video.customio.URLProtocolManager;

import java.io.IOException;

import java.util.HashMap;

public class Test {
    public Test() {
        super();
    }

    // this is a test to see if I can get away with decoding from the middle of the file by forcing demux format and codecs
    public void transcode(String source, String dest, String vcodec) throws InterruptedException, IOException {
        // let's add our HDFS protocol handler, so that we can read from HDFS
        URLProtocolManager mgr = URLProtocolManager.getManager();
        mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory());
        final Demuxer demuxer = Demuxer.make();
        demuxer.setForcedVideoCodec(Codec.ID.CODEC_ID_H264);
        demuxer.setForcedAudioCodec(Codec.ID.CODEC_ID_AC3);
        // This will be for both input and output mux/demux
        MuxerFormat muxFormat = MuxerFormat.guessFormat("matroska", null, null);
        DemuxerFormat demuxFormat = DemuxerFormat.findFormat("matroska");
        demuxer.setFlag(Container.Flag.FLAG_CUSTOM_IO, true);
        demuxer.setFlag(Container.Flag.FLAG_DISCARD_CORRUPT, true);
        demuxer.setFlag(Container.Flag.FLAG_NOBUFFER, true);
        demuxer.setFlag(Container.Flag.FLAG_NOFILLIN, true);
        try {
            demuxer.open(source, demuxFormat, true, false, null, null);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        final Muxer muxer = Muxer.make(dest, muxFormat, null);
        HashMap<Integer, Decoder> decoders = new HashMap<Integer, Decoder>();
        Encoder encH264=Encoder.make(Codec.findEncodingCodec(Codec.ID.CODEC_ID_H264));
        encH264.setPixelFormat(PixelFormat.Type.PIX_FMT_YUV420P);
        encH264.setWidth(1280);
        encH264.setHeight(692);
        encH264.setTimeBase(Rational.make(1001, 48000));
        encH264.setFlag(Coder.Flag.FLAG_GLOBAL_HEADER, true);
        encH264.open(null, null);
        Encoder encAC3_1=Encoder.make(Codec.findEncodingCodec(Codec.ID.CODEC_ID_AC3));
        encAC3_1.setSampleFormat(AudioFormat.Type.SAMPLE_FMT_FLTP);
        encAC3_1.setTimeBase(Rational.make(1, 48000));
        encAC3_1.setSampleRate(48000);
        encAC3_1.setChannels(6);
        encAC3_1.setChannelLayout(AudioChannel.Layout.CH_LAYOUT_5POINT1);
        encAC3_1.setFlag(Coder.Flag.FLAG_GLOBAL_HEADER, true);
        encAC3_1.open(null,null);
        Encoder encAC3_2=Encoder.make(Codec.findEncodingCodec(Codec.ID.CODEC_ID_AC3));
        encAC3_2.setSampleFormat(AudioFormat.Type.SAMPLE_FMT_FLTP);
        encAC3_2.setTimeBase(Rational.make(1, 48000));
        encAC3_2.setSampleRate(48000);
        encAC3_2.setChannels(6);
        encAC3_2.setChannelLayout(AudioChannel.Layout.CH_LAYOUT_5POINT1);
        encAC3_2.setFlag(Coder.Flag.FLAG_GLOBAL_HEADER, true);
        encAC3_2.open(null,null);
        Encoder encAC3_3=Encoder.make(Codec.findEncodingCodec(Codec.ID.CODEC_ID_AC3));
        encAC3_3.setSampleFormat(AudioFormat.Type.SAMPLE_FMT_FLTP);
        encAC3_3.setTimeBase(Rational.make(1, 48000));
        encAC3_3.setSampleRate(48000);
        encAC3_3.setChannels(6);
        encAC3_3.setChannelLayout(AudioChannel.Layout.CH_LAYOUT_5POINT1);
        encAC3_3.setFlag(Coder.Flag.FLAG_GLOBAL_HEADER, true);
        encAC3_3.open(null,null);
        muxer.addNewStream(encH264);
        muxer.addNewStream(encAC3_1);
        muxer.addNewStream(encAC3_2);
        muxer.addNewStream(encAC3_3);
        muxer.open(null, null);
        final MediaPacket packet = MediaPacket.make();
        while (demuxer.read(packet) >= 0) {
            /**
           * Now we have a packet, but we can only write packets that had decoders we knew what to do with.
           */
            int streamIdx = packet.getStreamIndex();
            if (!decoders.containsKey(streamIdx)) {
                // lets add the decoder to the map of decoders
                final DemuxerStream ds = demuxer.getStream(streamIdx);
                final Decoder d = ds.getDecoder();
                if (d != null) {
                    // neat; we can decode. Now let's see if this decoder can fit into the mp4 format.
                    if (!muxFormat.getSupportedCodecs().contains(d.getCodecID())) {
                        continue;
                    } else {
                        if (muxFormat.getFlag(MuxerFormat.Flag.GLOBAL_HEADER))
                            d.setFlag(Coder.Flag.FLAG_GLOBAL_HEADER, true);
                        d.open(null, null);
                        decoders.put(streamIdx, d);
                    }
                } else {
                    System.out.println("null decoder for demuxStream:"+ ds.toString());
                    continue;
                }

            }
            final Decoder d = decoders.get(streamIdx);

            if (d != null) {
                if (packet.isComplete() && d != null) {
                    muxer.write(packet, true);
                }
            }
        }

        // It is good practice to close demuxers when you're done to free
        // up file handles. Humble will EVENTUALLY detect if nothing else
        // references this demuxer and close it then, but get in the habit
        // of cleaning up after yourself, and your future girlfriend/boyfriend
        // will appreciate it.
        muxer.close();
        demuxer.close();
    }

    public static void main(String[] args) {
        Test test = new Test();
        try {
            test.transcode(args[0], args[1], args[2]);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
