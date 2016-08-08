package reactor.kafka.samples;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.util.StopWatch;

public class CustomExecutor extends ThreadPoolExecutor {

  private StopWatch sw = new StopWatch();
  private Consumer consumerCallback;

  public CustomExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    super.beforeExecute(t, r);
//    System.out.println("Perform beforeExecute() logic");
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    super.afterExecute(r, t);
    if (t != null) {
      System.out.println("Perform exception handler logic");
    }
    System.out.println("Perform afterExecute() logic. Time=" + System.currentTimeMillis());
  }

  public void setConsumerCallback(Consumer consumerCallback) {
    this.consumerCallback = consumerCallback;
  }

}