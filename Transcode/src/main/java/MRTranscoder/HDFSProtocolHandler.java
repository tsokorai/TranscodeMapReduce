package MRTranscoder;

import io.humble.video.customio.IURLProtocolHandler;

import java.io.IOException;

import java.net.URI;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSProtocolHandler implements IURLProtocolHandler {
    private static final Log LOG = LogFactory.getLog(HDFSProtocolHandler.class);
    private Path filePath = null;
    FSDataInputStream infile = null;
    FSDataOutputStream outfile = null;
    private boolean isOutput = false;
    private FileSystem fs = null;
    private long offset=0;

    public HDFSProtocolHandler() {
        this.filePath = null;
    }

    private String getOffset(String path) {
        String ret;
        Pattern pattern = Pattern.compile("^(.*)\\?(\\d+)$");
        Matcher matcher = pattern.matcher(path);
        if(matcher.matches()) {
            ret=matcher.group(1);
            offset=Long.parseLong(matcher.group(2));
        } else {
            ret=path;
            offset=0;
        }
        return ret;
    }
    public HDFSProtocolHandler(String path) {
        Pattern pattern = Pattern.compile("^(.*)\\?(\\d+)$");
        LOG.warn("Creating with path " + path);
        this.filePath = new Path(getOffset(path));
    }


    @Override
    public int open(String url, int flags) {
        LOG.warn("attempting to open " + url == null ? filePath : url + " with flags " + flags);
        
        if (flags == IURLProtocolHandler.URL_RDWR) {
            LOG.error("cannot open in read + write for HDFS files");
            return -1;
        }
        Configuration conf = new Configuration();
        if ((infile != null) || (outfile != null))
            this.close();

        //Get the filesystem - HDFS
        fs = null;
        try {
            fs = FileSystem.get(URI.create(getOffset(url)), conf);
        } catch (IOException e) {
            LOG.error("Cannot open HDFS fs ", e);
            return -1;
        }
        infile = null;
        outfile = null;
        if (flags == IURLProtocolHandler.URL_WRONLY_MODE) {
            isOutput = true;
            try {
                outfile = fs.create(filePath, true);
            } catch (IOException e) {
                LOG.error("Cannot create HDFS file ", e);
            }
            if(offset>0) {
                LOG.warn("The URL had an offset for write mode open, ignoring");
            }

        } else if (flags == IURLProtocolHandler.URL_RDONLY_MODE) {
            isOutput = false;
            try {
                infile = fs.open(filePath);
            } catch (IOException e) {
                LOG.error("Cannot open HDFS file ", e);
            }
            if(offset>0) {
                LOG.warn("The URL had an offset:"+offset);
                try {
                    infile.seek(offset);
                } catch (IOException e) {
                    LOG.error("Cannot seek ", e);
                }
            }

        } else {
            LOG.error("Invalid option flag " + flags);
            return -1;
        }
        return 0;
    }

    @Override
    public int read(byte[] b, int i) {
        if ((outfile != null) && (infile == null)) {
            LOG.error("Cannot read from write-only file");
            return -1;
        }
        int ret = -1;
        try {
            ret = infile.read(b, 0, i);
        } catch (IOException e) {
            LOG.error("Cannot read", e);
        }
        return ret;
    }

    @Override
    public int write(byte[] b, int i) {
        if ((infile != null) && (outfile == null)) {
            LOG.error("Cannot write on read-only file");
            return -1;
        }
        try {
            outfile.write(b, 0, i);
        } catch (IOException e) {
            LOG.error("Cannot read", e);
        }
        return 0;
    }

    @Override
    public long seek(long offset, int whence) {
        final long seek;
        if(outfile!=null) {
            return -1;
        }
        if (whence == SEEK_SET)
            seek = offset;
        else if (whence == SEEK_CUR)
            try {
                seek = infile.getPos() + offset;
            } catch (IOException e) {
                LOG.error("Cannot get file position for " + filePath, e);
                return -1;
            }
        else if ((whence == SEEK_END) || (whence == SEEK_SIZE)) {
            long size;
            try {
                size = fs.getFileStatus(filePath).getLen();
            } catch (IOException e) {
                LOG.error("Cannot get file size for " + filePath, e);
                return -1;
            }
            if (whence == SEEK_END) {
                seek = size + offset;
            } else {
                return size;
            }
        } else {
            LOG.error("invalid seek value " + whence + " for file: " + filePath);
            return -1;
        }
        try {
            infile.seek(seek);
        } catch (IOException e) {
            LOG.error("Cannot seek ", e);
            return -1;
        }
        return seek;
    }

    @Override
    public int close() {
        try {
            if (infile != null)
                infile.close();
            if (outfile != null) {
                outfile.flush();
                outfile.close();                
            }
        } catch (Exception e) {
            LOG.error("Cannot close ", e);
        } finally {
            infile = null;
            outfile = null;
            fs = null;
        }
        return 0;
    }

    @Override
    public boolean isStreamed(String string, int i) {
        return true;
    }
}
