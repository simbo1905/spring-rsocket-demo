package io.pivotal.rsocketserver;

import io.rsocket.SocketAcceptor;
import io.rsocket.metadata.WellKnownMimeType;
import lombok.extern.slf4j.Slf4j;
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
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.UUID;

@Slf4j
@Controller
public class ClusterTransport {
    public static final String CLIENT_ID = UUID.randomUUID().toString();
    private static final MimeType SIMPLE_AUTH = MimeTypeUtils.parseMimeType(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.getString());

    @Value("${demo.other.port}")
    private int otherPort;
    private Mono<RSocketRequester> requesterMono;

    public ClusterTransport(
            RSocketRequester.Builder rsocketRequesterBuilder,
            @Qualifier("rSocketStrategies")
                    RSocketStrategies rsocketStrategies) {
            SocketAcceptor responder = RSocketMessageHandler.responder(rsocketStrategies, new PeerHandler());
            UsernamePasswordMetadata user = new UsernamePasswordMetadata("user", "pass");
            this.requesterMono = rsocketRequesterBuilder
                    .setupRoute("peer-client")
                    .setupData(CLIENT_ID)
                    .setupMetadata(user, SIMPLE_AUTH)
                    .rsocketStrategies(builder1 ->
                            builder1.encoder(new SimpleAuthenticationEncoder()))
                    .rsocketConnector(connector -> {
                        connector.reconnect(Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(1)));
                        connector.acceptor(responder);
                    })
                    .connectTcp("localhost", otherPort);
    }

    Mono<RSocketRequester> getRSocketRequesterMono() {
        return this.requesterMono;
    }
}
