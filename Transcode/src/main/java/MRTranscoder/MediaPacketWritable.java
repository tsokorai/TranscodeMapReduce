package MRTranscoder;

import io.humble.ferry.Buffer;
import io.humble.video.MediaPacket;

import io.humble.video.Rational;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

public class MediaPacketWritable implements Writable {
    private static final Log LOG = LogFactory.getLog(MediaPacketWritable.class);
    MediaPacket packet=null;
    public MediaPacketWritable() {
        packet=MediaPacket.make();
        
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        int bfrSize=((packet.getData()!=null) && (packet!=null)) ?packet.getData().getBufferSize():0;
        dataOutput.writeInt(bfrSize);
        if(packet!=null) {
            dataOutput.writeLong(packet.getConvergenceDuration());
            dataOutput.writeLong(packet.getDts());
            dataOutput.writeLong(packet.getDuration());
            dataOutput.writeLong(packet.getPosition());
            dataOutput.writeLong(packet.getPts());
            dataOutput.writeLong(packet.getTimeStamp());
            dataOutput.writeInt(packet.getFlags());
            dataOutput.writeInt(packet.getStreamIndex());
            dataOutput.writeInt((packet.getTimeBase()!=null)?packet.getTimeBase().getNumerator():0);
            dataOutput.writeInt((packet.getTimeBase()!=null)?packet.getTimeBase().getDenominator():0);            
        }
        if(bfrSize>0)
            dataOutput.write(packet.getData().getByteArray(0,bfrSize));
        LOG.info("Wrote (serialized) packet");
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int bfrSize=dataInput.readInt();
        if(bfrSize>0)
        {
            packet.setConvergenceDuration(dataInput.readLong());
            packet.setDts(dataInput.readLong());
            packet.setDuration(dataInput.readLong());
            packet.setPosition(dataInput.readLong());
            packet.setPts(dataInput.readLong());
            packet.setTimeStamp(dataInput.readLong());
            packet.setFlags(dataInput.readInt());
            packet.setStreamIndex(dataInput.readInt());
            int num=dataInput.readInt();
            int denom=dataInput.readInt();
            if((num!=0) || (denom!=0))
                packet.setTimeBase(Rational.make(num, denom));

            byte bfr[]= new byte[bfrSize];
            dataInput.readFully(bfr);
            Buffer buffer = Buffer.make(null,bfr,0,bfrSize);
            packet.delete();
            packet=MediaPacket.make(buffer);
        } else {
            packet.reset(0);
        }
        LOG.info("Read (deserialized) packet");
    }

    public MediaPacket getPacket() {
        return packet;
    }
}
