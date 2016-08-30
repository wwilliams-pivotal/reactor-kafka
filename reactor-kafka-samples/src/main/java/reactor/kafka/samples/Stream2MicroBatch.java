package reactor.kafka.samples;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.util.StopWatch;

public class Stream2MicroBatch {

  private Region<String, String> region = null;

  private int recordThreshhold = 10000;

  private AtomicInteger recordCount = new AtomicInteger(0);
  private BlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<>();

  /* start with a single thread and let it go up from there */
  CustomExecutor executor = new CustomExecutor(1, 20, 5000, TimeUnit.MILLISECONDS, blockingQueue);
  private StopWatch sw = new StopWatch();
  private AtomicInteger bufferCount = new AtomicInteger(0);
  private String firstKey = "";
  private boolean isFirstKey = true;

  /* Consumer callback with the set of keys being persisted */
  private Consumer consumerCallback;
  @SuppressWarnings("rawtypes")
  Map[] buffers = new ConcurrentHashMap[1000];
  private int bufferIx = 0;

  public Stream2MicroBatch(Region<String, String> region, Consumer consumerCallback) {
    this.region = region;
    this.consumerCallback = consumerCallback;

    executor.setRejectedExecutionHandler(new RejectedExecutionHandler() {
      @Override
      public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        System.out.println("PutAllTask Rejected : " + ((PutAllTask) r).getName());
        System.out.println("Waiting for a second !!");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        System.out.println("Lets add another time : " + ((PutAllTask) r).getName());
        executor.execute(r);
      }
    });
    executor.setConsumerCallback(consumerCallback);
    // Let start all core threads initially
    executor.prestartAllCoreThreads();

    initializeBuffers();
  }
  
  private void initializeBuffers() {
    for (int i=0; i<buffers.length; i++) {
      buffers[i] = new ConcurrentHashMap<String, String>();
    }
  }

  /**
   * put a record and create a microbatch for execution when puts exceed either size or time
   */
  @SuppressWarnings("unchecked")
  //public void put(String key, String value) {
  public void put(String key, Long value) {

    /*
     * capture first key to identify the name of the batch we are collecting
     */
    if (isFirstKey) {
      firstKey = key;
      sw.start();
      isFirstKey = false;
    }

    buffers[bufferIx].put(key, value);

    /*
     * submit microbatch every second or record count threshold
     */
    if (recordCount.incrementAndGet() >= recordThreshhold || sw.elapsedTimeMillis() > 1000) {
      submitMicrobatch();
    }
  }
  
  /*
   * release the microbatch to send to GemFire
   */
  @SuppressWarnings("unchecked")
  private void submitMicrobatch() {
    bufferIx = bufferCount.get();
    System.out.println("writing " + firstKey + " and bufferIx=" + bufferIx);
    PutAllTask task = new PutAllTask(buffers[bufferIx], region, firstKey, consumerCallback);
    executor.submit(task);
    recordCount.set(0);
    
    // reset microbatch timer
    sw.stop();
    sw.start();
    bufferIx = bufferCount.incrementAndGet();
    if (bufferIx >= buffers.length) {
      bufferCount.set(0);
      bufferIx = 0;
    }
    isFirstKey = true;
  }
  
  public void dispose() {
    executor.shutdown();
  }

}
