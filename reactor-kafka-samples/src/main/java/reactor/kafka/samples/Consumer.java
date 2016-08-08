package reactor.kafka.samples;

import java.util.Set;

public interface Consumer {

  void callback(Set<String> keysWritten);
}
