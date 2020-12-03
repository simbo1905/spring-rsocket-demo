package io.pivotal.rsocketserver;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.MonoSink;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Controller
public class ReplicationNexus<T> implements Closeable {
    final Map<String, MonoSink<T>> requests = new ConcurrentHashMap<>();

    final AtomicBoolean shutdown = new AtomicBoolean(false);

    @Override
    public void close() throws IOException {
        shutdown.set(true);
        requests.values().stream().forEach(sink -> sink.error(new IOException(this.getClass().getCanonicalName()+" has closed.")));
        requests.clear();
    }

    public boolean registerRequest(String v, MonoSink<T> sink) {
        boolean running = shutdown.get() == false;
        if( running ) {
            requests.put(v, sink);
        }
        return running;
    }

    public boolean complete(String uuid, T message) {
        Optional<MonoSink<T>> sink = Optional.of(requests.get(uuid));
        if( sink.isPresent() ){
            sink.get().success(message);
        }
        return sink.isPresent();
    }
}
