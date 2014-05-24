package org.w007.activemq;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.filter.DestinationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class DestinationReapingBroker extends BrokerFilter {
  /**
   * Milliseconds that matching destinations will be allowed to live.
   */
  private final long destinationTimeToLive;

  /**
   * This service thread that periodically checks whether any destinations are up for reaping.
   */
  private final ScheduledExecutorService scheduler = createScheduler();

  /**
   * Handle to the scheduled task so that we may cancel it when the broker stops.
   */

  private ScheduledFuture<?> task;

  /**
   * The actual Runnable executed by {@link #scheduler}.
   */
  private final DestinationReaperTask reaperTask;

  /**
   * If a queue matches this filter, it will be reaped.
   */
  private final DestinationFilter queueFilter;

  /**
   * If a topic matches this filter, it will be reaped.
   */
  private final DestinationFilter topicFilter;

  /**
   * Destinations that match the pattern are put in this queue, which {@link #reaperTask}
   * checks for expired destinations.
   */
  private final Queue<ExecutionOrder> executionOrders;

  private final Logger log = LoggerFactory.getLogger(DestinationReapingBroker.class);

  /**
   * This keeps tabs on destinations as they are added to the broker. If they
   * match a pattern, it will ensure that the destination is deleted after it
   * has been around for a period of time.
   */
  public DestinationReapingBroker(Broker next, String pattern, long destinationTimeToLive) {
    super(next);
    this.destinationTimeToLive = destinationTimeToLive;
    reaperTask = new DestinationReaperTask();
    // A DestinationFilter given a Queue will not match a Topic with an otherwise matching name, and vice versa,
    // so we have a filter for each, with the same pattern.
    queueFilter = DestinationFilter.parseFilter(new ActiveMQQueue(pattern));
    topicFilter = DestinationFilter.parseFilter(new ActiveMQTopic(pattern));
    executionOrders = new ArrayDeque<ExecutionOrder>();
  }

  /**
   * Hook for knowing when destinations are added.
   */
  @Override
  public Destination addDestination(ConnectionContext context, ActiveMQDestination destination, boolean createIfTemporary) throws Exception {
    Destination created = super.addDestination(context, destination, createIfTemporary);
    if (shouldBeReaped(destination)) {
      addExecutionOrder(destination);
    }
    return created;
  }

  /**
   * Hook for knowing when destinations are added.
   */
  @Override
  public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
    super.addDestinationInfo(context, info);
    ActiveMQDestination destination = info.getDestination();
    if (shouldBeReaped(destination)) {
      addExecutionOrder(destination);
    }
  }

  long getDestinationTimeToLiveMillis() {
    return destinationTimeToLive;
  }

  /**
   * The destination's milliseconds are numbered.
   */
  private void addExecutionOrder(ActiveMQDestination destination) {
    long timeToDieMillis = System.currentTimeMillis() + destinationTimeToLive;
    log.debug("Adding execution order for {} at time {}", destination, timeToDieMillis);
    synchronized (executionOrders) {
      executionOrders.add(new ExecutionOrder(destination, timeToDieMillis));
    }
  }

  /**
   * @return whether the destination is subject to reaping
   */
  boolean shouldBeReaped(ActiveMQDestination destination) {
    return queueFilter.matches(destination) || topicFilter.matches(destination);
  }

  @Override
  public void start() throws Exception {
    super.start();
    scheduleReaping();
  }

  @Override
  public void stop() throws Exception {
    stopReaping();
    super.stop();
  }

  private void scheduleReaping() {
    long rate = destinationTimeToLive / 5;
    task = scheduler.scheduleAtFixedRate(reaperTask, rate, rate, TimeUnit.MILLISECONDS);
  }

  private void stopReaping() {
    if (task != null)
      task.cancel(false);
  }

  private ScheduledExecutorService createScheduler() {
    return Executors.newSingleThreadScheduledExecutor();
  }

  private class DestinationReaperTask implements Runnable {
    private final Logger log = LoggerFactory.getLogger(DestinationReaperTask.class);

    @Override
    public void run() {
      try {
        log.debug("Reaping destinations");
        final long currentTimeMillis = System.currentTimeMillis();
        final BrokerService brokerService = getBrokerService();
        synchronized (executionOrders) {
          ExecutionOrder order;
          while ((order = executionOrders.peek()) != null) {
            if (order.timeToDieMillis <= currentTimeMillis) {
              executionOrders.remove();
              removeDestination(brokerService, order.destination);
            } else {
              log.debug("Order {} is in the future as of {}; delta: {}",
                  order, currentTimeMillis, order.timeToDieMillis - currentTimeMillis);
              // Elements in executionOrders are in chronological order, so we can stop looking
              // for destinations to delete once one is yet to expire.
              break;
            }
          }
        }
      } catch (Exception e) {
        log.error("Error while finding destinations to reap: {}", e.getMessage());
        log.debug("Stack trace", e);
      }
    }

    private void removeDestination(BrokerService brokerService, ActiveMQDestination destination) {
      try {
        log.info("Removing destination {}", destination);
        brokerService.removeDestination(destination);
      } catch (Exception e) {
        log.error("Could not remove destination {}: .", destination, e.getMessage());
        log.debug("Stack trace", e);
      }
    }
  }

}
