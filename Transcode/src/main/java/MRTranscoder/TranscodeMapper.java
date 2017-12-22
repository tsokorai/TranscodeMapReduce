package MRTranscoder;

import io.humble.video.AudioChannel;
import io.humble.video.AudioFormat;
import io.humble.video.Codec;
import io.humble.video.Decoder;
import io.humble.video.Encoder;
import io.humble.video.MediaAudio;
import io.humble.video.MediaAudioResampler;
import io.humble.video.MediaDescriptor;
import io.humble.video.MediaPacket;

import io.humble.video.MediaPicture;
import io.humble.video.MediaPictureResampler;
import io.humble.video.MediaResampler;
import io.humble.video.Muxer;
import io.humble.video.MuxerFormat;

import io.humble.video.PixelFormat;

import io.humble.video.Rational;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TranscodeMapper extends Mapper<MediaStreamKeyWritable, MediaPacketWritable, MediaStreamKeyWritable, MediaPacketWritable> {
    private static final Log LOG = LogFactory.getLog(TranscodeMapper.class);
    private Encoder videoEncoder = null;
    private Decoder videoDecoder = null;


    public void map(MediaStreamKeyWritable key, MediaPacketWritable value, Context context) throws IOException,
                                                                                                   InterruptedException {
        // here we do the codec conversion (i.e., transcoding) ... it will be re-muxed in the reducer
        MediaPacketWritable destValue=new MediaPacketWritable();
        // this is the codec we got from the job command line (currently only for video, the audio ones are hardwired)
        if (context.getConfiguration().getInt("mediaStreamCodecType_" + key.getStream(), 0) ==
            MediaDescriptor.Type.MEDIA_VIDEO.swigValue()) {
            //LOG.info("Decoding video");
            Codec codec = Codec.findEncodingCodecByName(context.getConfiguration().get("destinationCodec"));
            PixelFormat.Type pixelFormat =
            PixelFormat.Type.swigToEnum(context.getConfiguration()
                                        .getInt("mediaStreamCodecPixelFormat_" + key.getStream(), 0));
            if(videoEncoder==null)
            {
                videoEncoder = Encoder.make(codec);
                videoEncoder.setPixelFormat(pixelFormat);
                videoEncoder.setWidth(640);
                videoEncoder.setHeight(480);
                Rational tb =
                    Rational.make(context.getConfiguration().getInt("mediaStreamCodecTimeBaseNum_" + key.getStream(), 0),
                                  context.getConfiguration().getInt("mediaStreamCodecTimeBaseDenom_" + key.getStream(), 0));
                if(tb!=null)
                {
                    videoEncoder.setTimeBase(tb);
                } else {
                    LOG.warn("TIMEBASE NULL");
                }
                videoEncoder.open(null, null);
            }
            int w = context.getConfiguration().getInt("mediaStreamCodecWidth_" + key.getStream(), 0), 
                h = context.getConfiguration().getInt("mediaStreamCodecHeight_" + key.getStream(), 0);
            Codec.ID codecID=Codec.ID.swigToEnum(context.getConfiguration().getInt("mediaStreamCodecID_" + key.getStream(), 0));
            MediaPicture picture = MediaPicture.make(w, h, pixelFormat);
            Rational tb =
                Rational.make(context.getConfiguration().getInt("mediaStreamCodecTimeBaseNum_" + key.getStream(), 0),
                              context.getConfiguration().getInt("mediaStreamCodecTimeBaseDenom_" + key.getStream(), 0));
            if(videoDecoder==null)
            {
                videoDecoder = Decoder.make(Codec.findDecodingCodec(codecID));
                videoDecoder.setWidth(w);
                videoDecoder.setHeight(h);
                videoDecoder.setPixelFormat(pixelFormat);
                if(tb!=null)
                {
                    videoDecoder.setTimeBase(tb);
                } else {
                    LOG.warn("TIMEBASE NULL");
                }
                videoDecoder.open(null, null);
            }

            
            int off = 0,ret=0;
            do {
                try {
                    ret= videoDecoder.decodeVideo(picture, value.getPacket(), off);
                    if(ret>=0)
                        off += ret ;
                } catch (Exception e) {
                    LOG.warn("Ignoring last exception, ignoring current packet");
                    return;
                }
                if (picture.isComplete()) {
                    break;
                }
            } while ((off < value.getPacket().getSize()) && (ret>=0));
            if(!picture.isComplete()) {
                LOG.warn("Ignoring k/v:"+key+","+value+ " beacause the frame couldn't be completed");
                return;                
            }
            MediaPicture destPicture = MediaPicture.make(640, 480, pixelFormat);
            MediaPictureResampler resampler =
                MediaPictureResampler.make(640, 480, pixelFormat, w, h, pixelFormat,
                                           MediaPictureResampler.Flag.FLAG_AREA.swigValue());
            resampler.open();
            resampler.resamplePicture(destPicture, picture);
            if(tb!=null)
                destPicture.setTimeBase(tb);
            //LOG.info("Pic resampled");
            
            videoEncoder.encodeVideo(destValue.getPacket(),destPicture);
            //LOG.info("Pic encoded");
            // let's use the Play time to set the new packet order
            MediaStreamKeyWritable newKey=new MediaStreamKeyWritable(key.getStream(),value.getPacket().getPts());
            context.write(newKey, destValue);
            resampler.delete();
            picture.delete();
            destPicture.delete();
        } /*  else if(context.getConfiguration().getInt("mediaStreamCodecType_" + key.getStream(), 0) ==
                  MediaDescriptor.Type.MEDIA_AUDIO.swigValue()){
            LOG.info("Decoding audio");
            Codec codec = Codec.findEncodingCodecByName("libmp3lame");
            encoder = Encoder.make(codec);
            encoder.setSampleFormat(AudioFormat.Type.SAMPLE_FMT_S16P);
            encoder.setChannelLayout(AudioChannel.Layout.CH_LAYOUT_STEREO);
            encoder.setSampleRate(44100);
            int inSrate=context.getConfiguration().getInt("mediaStreamCodecSampleRate_" + key.getStream(), 0);
            int inCh=context.getConfiguration().getInt("mediaStreamCodecChannels_" + key.getStream(), 0);
            AudioChannel.Layout inChLayout=AudioChannel.Layout.swigToEnum(context.getConfiguration()
                                                            .getInt("mediaStreamCodecChannelLayout_" + key.getStream(), 0));
            AudioFormat.Type inType=AudioFormat.Type.swigToEnum(context.getConfiguration()
                                                            .getInt("mediaStreamCodecSampleFormat_" + key.getStream(), 0));
            Codec.ID codecID=Codec.ID.swigToEnum(context.getConfiguration()
                                                            .getInt("mediaStreamCodecID_" + key.getStream(), 0));
            decoder =
                Decoder.make(Codec.findDecodingCodec(codecID));
            decoder.setChannelLayout(inChLayout);
            decoder.setSampleRate(inSrate);
            decoder.setChannels(context.getConfiguration()
                                                            .getInt("mediaStreamCodecChannels_" + key.getStream(), 0));
            Rational tb =
                Rational.make(context.getConfiguration().getInt("mediaStreamCodecTimeBaseNum_" + key.getStream(), 0),
                              context.getConfiguration().getInt("mediaStreamCodecTimeBaseDenom_" + key.getStream(), 0));
            if(tb!=null)
            {
                decoder.setTimeBase(tb);
                encoder.setTimeBase(tb);
            } else {
                LOG.warn("TIMEBASE NULL");
            }
            decoder.open(null, null);
            MediaAudio audio=MediaAudio.make(decoder.getFrameSize(), inSrate, inCh, inChLayout, inType);
            LOG.info("audio created:"+audio);
            int off = 0,ret=0;
            do {
                try {
                    ret = decoder.decodeAudio(audio, value.getPacket(), off);
                    if(ret>=0)
                        off += ret;
                } catch (Exception e) {
                    LOG.warn("Ignoring k/v:"+key+","+value+ " beacause of: ",e);
                    return;
                }
                if (audio.isComplete()){
                    LOG.info("audio deco complete");
                    break;                    
                }
            } while ((off < value.getPacket().getSize()) && (ret>=0));
            if(!audio.isComplete()) {
                LOG.warn("Ignoring k/v:"+key+","+value+ " beacause the audio frame couldn't be completed");
                return;                
            }
            encoder.open(null, null);
            MediaAudio destAudio = MediaAudio.make(encoder.getFrameSize(), 44100, 2, AudioChannel.Layout.CH_LAYOUT_STEREO, AudioFormat.Type.SAMPLE_FMT_S16P);
            MediaAudioResampler resampler =
                MediaAudioResampler.make(AudioChannel.Layout.CH_LAYOUT_STEREO, 44100, AudioFormat.Type.SAMPLE_FMT_S16P,inChLayout,inSrate,inType);
            resampler.open();
            resampler.resampleAudio(destAudio, audio);
            LOG.info("Audio resampled");
            
            encoder.encodeAudio(destValue.getPacket(),destAudio);
            LOG.info("Audio encoded");

            context.write(key, destValue);
          //  decoder.delete();
         //   encoder.delete();
        } */
            
            
    }

    @Override
    protected void cleanup(Mapper<MediaStreamKeyWritable, MediaPacketWritable, MediaStreamKeyWritable, MediaPacketWritable>.Context context) throws IOException,
                                                                                                                                                    InterruptedException {
        if(videoEncoder!=null)
            videoEncoder.delete();
        if(videoDecoder!=null)
            videoDecoder.delete();
        super.cleanup(context);
    }

}
