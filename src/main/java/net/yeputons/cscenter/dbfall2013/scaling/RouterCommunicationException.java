package net.yeputons.cscenter.dbfall2013.scaling;

/**
 * Created with IntelliJ IDEA.
 * User: e.suvorov
 * Date: 20.10.13
 * Time: 0:10
 * To change this template use File | Settings | File Templates.
 */
public class RouterCommunicationException extends RuntimeException {
    public RouterCommunicationException(Throwable cause) {
        super(cause);
    }

    public RouterCommunicationException(String message) {
        super(message);
    }
}
