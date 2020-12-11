package io.pivotal.rsocketserver;

import io.pivotal.rsocketserver.data.Message;
import io.rsocket.SocketAcceptor;
import io.rsocket.metadata.WellKnownMimeType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.security.rsocket.metadata.SimpleAuthenticationEncoder;
import org.springframework.security.rsocket.metadata.UsernamePasswordMetadata;
import org.springframework.stereotype.Controller;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.pivotal.rsocketserver.RSocketController.RESPONSE;
import static io.pivotal.rsocketserver.RSocketController.SERVER;

@Slf4j
@Controller
public class ClusterTransport {
    private static final String CLIENT_ID = UUID.randomUUID().toString();
    private static final MimeType SIMPLE_AUTH = MimeTypeUtils.parseMimeType(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.getString());

    @Value("${demo.other.port}")
    private int otherPort;
    private RSocketRequester rsocketRequester;
    private RSocketRequester.Builder rsocketRequesterBuilder;
    private RSocketStrategies rsocketStrategies;

    ExecutorService executor = Executors.newSingleThreadExecutor();

    /**
     * Optional.empty() is a poison pill that will cause the paxos thread to shutdown.
     */
    public BlockingQueue<Optional<String>> messageQueue = new LinkedBlockingDeque<>();

    public ClusterTransport(
            RSocketRequester.Builder builder,
            @Qualifier("rSocketStrategies")
                    RSocketStrategies strategies) {
        this.rsocketRequesterBuilder = builder;
        this.rsocketStrategies = strategies;

        executor.submit(()-> {
            Optional<String> v;
            do{
                try {
                    v = messageQueue.take();
                    reconnect();
                } catch (InterruptedException e) {
                    log.warn("thread interrupted");
                    v = Optional.empty();
                }
                v.stream().forEach(m->process(m));
            } while( v.isPresent() );
            executor.shutdownNow();
        } );
    }

    private AtomicBoolean requiresReconnect = new AtomicBoolean(true);

    private synchronized void reconnect() {
        if( requiresReconnect.get() == false ){
            return;
        }
        if( this.rsocketRequester != null ){
            this.rsocketRequester.rsocket().dispose();
        }
        SocketAcceptor responder = RSocketMessageHandler.responder(rsocketStrategies, new PeerHandler());
        UsernamePasswordMetadata user = new UsernamePasswordMetadata("user", "pass");
        this.rsocketRequester = rsocketRequesterBuilder
                .setupRoute("peer-client")
                .setupData(CLIENT_ID)
                .setupMetadata(user, SIMPLE_AUTH)
                .rsocketStrategies(builder ->
                        builder.encoder(new SimpleAuthenticationEncoder()))
                .rsocketConnector(connector -> {
                    connector.acceptor(responder);
                })
                .connectTcp("localhost", otherPort)
                .block();

        this.rsocketRequester.rsocket()
                .onClose()
                .doOnError(error -> log.warn("Peer outbound CLOSED"))
                .doFinally(consumer -> {
                    log.info("Peer outbound DISCONNECTED");
                    requiresReconnect.set(true);
                })
                .subscribe();
        requiresReconnect.set(false);
    }

    @Autowired
    ReplicationNexus<Message> replicationNexus;

    private void process(final String uuid) {
        String response = this.rsocketRequester
                .route("pong")
                .data(uuid)
                .retrieveMono(String.class)
                .block();
        replicationNexus.complete(response, new Message(SERVER, RESPONSE));
    }
}
