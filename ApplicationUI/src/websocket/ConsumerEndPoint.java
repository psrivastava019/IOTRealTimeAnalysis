package websocket;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.websocket.EncodeException;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;

@ServerEndpoint(value = "/ConsumerEndPoint", decoders = MessageDecoder.class, encoders = MessageEncoder.class)
public class ConsumerEndPoint {
    private Session session;
    private static final Set<ConsumerEndPoint> chatEndpoints = new CopyOnWriteArraySet<>();
    private static HashMap<String, String> users = new HashMap<>();

    @OnOpen
    public void onOpen(Session session) throws IOException, EncodeException {
    	System.out.println("client connected:"+session);
    	onMessage(session, new Message());
    }

    @OnMessage
    public void onMessage(Session session, Message message) throws IOException, EncodeException {
    	System.out.println("onmessage"+DataStore.jsonStringData);
        message.setFrom(users.get(session.getId()));
        message.setContent(DataStore.jsonStringData);
        session.getBasicRemote().sendObject(message);
    }

    @OnClose
    public void onClose(Session session) throws IOException, EncodeException {
        System.out.println("closed connection:"+session);
    }

    @OnError
    public void onError(Session session, Throwable throwable) {
        // Do error handling here
    }

    

}
