package org.w007.activemq;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.filter.DestinationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class DestinationReapingBroker extends BrokerFilter {
  private final long destinationTimeToLive;
  private final String destination;
  private volatile ScheduledExecutorService scheduler = createScheduler();
  private ScheduledFuture<?> task;
  private final DestinationReaperTask reaperTask;
  private Logger log = LoggerFactory.getLogger(DestinationReapingBroker.class);

  public DestinationReapingBroker(Broker next, String destination, long destinationTimeToLive) {
    super(next);
    this.destination = destination;
    this.destinationTimeToLive = destinationTimeToLive;
    reaperTask = new DestinationReaperTask();
  }

  @Override
  public void start() throws Exception {
    super.start();
    scheduleReaping();
  }

  @Override
  public void stop() throws Exception {
    if (task != null)
      task.cancel(false);
    super.stop();
  }

  List<ActiveMQDestination> getMatchingDestinations() throws Exception {
    DestinationFilter queueFilter = DestinationFilter.parseFilter(new ActiveMQQueue(destination));
    DestinationFilter topicFilter = DestinationFilter.parseFilter(new ActiveMQTopic(destination));
    List<ActiveMQDestination> matching = new ArrayList<ActiveMQDestination>();
    log.debug("{}", next);
    for (ActiveMQDestination activeDestination : next.getDestinations()) {
      if (queueFilter.matches(activeDestination) || topicFilter.matches(activeDestination)) {
        matching.add(activeDestination);
      }
    }
    return matching;
  }

  private void scheduleReaping() {
    task = scheduler.scheduleAtFixedRate(reaperTask, destinationTimeToLive, destinationTimeToLive, TimeUnit.MILLISECONDS);
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
        BrokerService brokerService = next.getBrokerService();
        for (ActiveMQDestination toReap : getMatchingDestinations()) {
          try {
            log.info("Removing destination {}", toReap);
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
  }
}
