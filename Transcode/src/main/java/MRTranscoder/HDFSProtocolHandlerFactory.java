package MRTranscoder;

import io.humble.video.customio.IURLProtocolHandler;
import io.humble.video.customio.IURLProtocolHandlerFactory;

public class HDFSProtocolHandlerFactory implements IURLProtocolHandlerFactory {
    public HDFSProtocolHandlerFactory() {
        super();
    }

    @Override
    public IURLProtocolHandler getHandler(String protocol, String url, int i) {

        return new HDFSProtocolHandler(url);
    }
}
