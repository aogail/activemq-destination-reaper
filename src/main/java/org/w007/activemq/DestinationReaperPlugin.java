package org.w007.activemq;


import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.filter.DestinationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * An ActiveMQ broker plugin that removes destinations matching
 * a pattern after a certain time has passed.
 *
 * @org.apache.xbean.XBean element="destinationReaperBrokerPlugin"
 */
public class DestinationReaperPlugin implements BrokerPlugin, Runnable {
  public static final Logger log = LoggerFactory.getLogger(DestinationReaperPlugin.class);
  private volatile long timeoutMillis = TimeUnit.DAYS.toMillis(1);
  private volatile String destination;
  private volatile Broker broker;
  private volatile ScheduledExecutorService scheduler = createScheduler();
  private ScheduledFuture<?> task;

  public Broker installPlugin(Broker broker) throws Exception {
    log.info("Installing Destination Reaper broker plugin for destination {} and timeout {} ms",
        destination, timeoutMillis);
    this.broker = broker;
    scheduleReaping();
    return broker;
  }

  /**
   * After this time out period, the destination(s) matching {@link #getDestination()}
   * will be deleted.
   * <p/>
   * The default is one day.
   */
  public long getTimeoutMillis() {
    return timeoutMillis;
  }

  /**
   * Set the period of time for which destinations matching {@link #getDestination()}
   * will live.
   *
   * @param timeoutMillis time in milliseconds
   */
  public void setTimeoutMillis(long timeoutMillis) {
    this.timeoutMillis = timeoutMillis;
  }

  /**
   * Set the destination name or pattern that defines which destinations will be reaped.
   */
  public void setDestination(String destination) {
    this.destination = destination;
  }

  /**
   * This pattern defines the destinations that will be deleted after the timeout period.
   */
  public String getDestination() {
    return destination;
  }

  @Override
  public void run() {
    try {
      BrokerService brokerService = broker.getBrokerService();
      for (ActiveMQDestination toReap : getMatchingDestinations()) {
        try {
          brokerService.removeDestination(toReap);
        } catch (Exception e) {
          log.error("Could not remove destination {}: .", toReap, e.getMessage());
          log.debug("Stack trace", e);
        }
      }
    } catch (Exception e) {
      log.error("Error while finding destinations to reap: {}", e.getMessage());
      log.debug("Stack trace", e);
    }
  }

  List<ActiveMQDestination> getMatchingDestinations() throws Exception {
    DestinationFilter queueFilter = DestinationFilter.parseFilter(new ActiveMQQueue(destination));
    DestinationFilter topicFilter = DestinationFilter.parseFilter(new ActiveMQTopic(destination));
    List<ActiveMQDestination> matching = new ArrayList<ActiveMQDestination>();
    for (ActiveMQDestination activeDestination : broker.getDestinations()) {
      if (queueFilter.matches(activeDestination) || topicFilter.matches(activeDestination)) {
        matching.add(activeDestination);
      }
    }
    return matching;
  }

  private void scheduleReaping() {
    // The unit tests install the plugin more than once, so we cancel any outstanding task now.
    if (task != null)
      task.cancel(true);
    task = scheduler.scheduleAtFixedRate(this, timeoutMillis, timeoutMillis, TimeUnit.MILLISECONDS);
  }

  private ScheduledExecutorService createScheduler() {
    return Executors.newSingleThreadScheduledExecutor();
  }
}
