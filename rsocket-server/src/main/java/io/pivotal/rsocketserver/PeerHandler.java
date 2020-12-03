package io.pivotal.rsocketserver;

import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.MessageMapping;
import reactor.core.publisher.Flux;

import java.time.Duration;

@Slf4j
class PeerHandler {

    @MessageMapping("peer-status")
    public Flux<String> statusUpdate(String status) {
        log.info("Peer OPENED {}", status);
        return Flux.interval(Duration.ofSeconds(5)).map(index -> String.valueOf(Runtime.getRuntime().freeMemory()));
    }
}
