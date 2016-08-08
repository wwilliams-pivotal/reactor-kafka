package reactor.kafka.samples;

import java.util.Map;

import org.springframework.util.StopWatch;

import com.gemstone.gemfire.cache.Region;

public class PutAllTask implements Runnable {

  private Map<String, String> buffer;
  private Region<String, String> region;
  private String name;
  private Consumer consumerCallback;
  private StopWatch sw = new StopWatch();

  public PutAllTask(Map<String, String> buffer, Region<String, String> region, String name, Consumer consumerCallback) {
    super();
    this.name = name;
    this.buffer = buffer;
    this.region = region;
    this.consumerCallback = consumerCallback;
  }

  @Override
  public void run() {
    try {
 //     System.out.println("I am about to write " + buffer.size() + " records and name=" + name);
      sw.start();
      region.putAll(buffer);
      sw.stop();
      System.out.println("I wrote " + buffer.size() + " records in " + sw.getTotalTimeMillis());
      consumerCallback.callback(buffer.keySet());
      buffer = null;
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public String getName() {
    return name;
  }
}
