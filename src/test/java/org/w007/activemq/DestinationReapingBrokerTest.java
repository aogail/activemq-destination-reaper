package org.w007.activemq;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class DestinationReapingBrokerTest extends ActiveMqTestSupport {
  @Test
  public void canConstruct() throws Exception {
    DestinationReapingBroker broker = new DestinationReapingBroker(mock(Broker.class), "foo", TimeUnit.SECONDS.toMillis(1));
    assertThat(broker, instanceOf(BrokerFilter.class));
  }

  @Test
  public void findsMatchingDestinations() throws Exception {
    Broker broker = startEmbeddedBroker();
    try {
      addQueues(broker, "queue.one", "queue.two", "other.queue.one");
      addTopics(broker, "topic.one", "topic.two", "other.topic.one");

      DestinationReapingBroker reapingBroker = new DestinationReapingBroker(broker, "queue.>", TimeUnit.SECONDS.toMillis(1));

      assertThat(reapingBroker.getMatchingDestinations(),
          containsInAnyOrder((ActiveMQDestination) new ActiveMQQueue("queue.one"), new ActiveMQQueue("queue.two")));

    } finally {
      broker.stop();
    }
  }

  @Test
  public void removesDestinationsAfterTimeoutPasses() throws Exception {
    BrokerService brokerService = createEmbeddedBrokerService();
    try {
      brokerService.setPlugins(new BrokerPlugin[]{new DestinationReaperPlugin("queue.>", TimeUnit.SECONDS.toMillis(1))});
      brokerService.start();
      Broker broker = brokerService.getBroker();

      addQueues(broker, "queue.one", "queue.two", "other.queue.one");
      addTopics(broker, "topic.one", "topic.two", "other.topic.one");
      ActiveMQDestination[] originalDestinations = broker.getDestinations();


      List<ActiveMQDestination> currentDestinations = Arrays.asList(broker.getDestinations());
      assertThat(currentDestinations, containsInAnyOrder(originalDestinations));

      TimeUnit.SECONDS.sleep(2);

      currentDestinations = Arrays.asList(broker.getDestinations());
      ActiveMQDestination[] expectedDestinations = {
          new ActiveMQTopic("topic.one"),
          new ActiveMQTopic("topic.two"),
          new ActiveMQTopic("other.topic.one"),
          new ActiveMQQueue("other.queue.one")
      };

      assertThat(currentDestinations, containsInAnyOrder(expectedDestinations));
    } finally {
      brokerService.stop();
    }
  }

}
