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
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;

public class DestinationReapingBrokerTest extends ActiveMqTestSupport {
  @Test
  public void canConstruct() throws Exception {
    DestinationReapingBroker broker = new DestinationReapingBroker(mock(Broker.class), "foo", TimeUnit.SECONDS.toMillis(1));
    assertThat(broker, instanceOf(BrokerFilter.class));
  }

  @Test
  public void findsMatchingDestinations() throws Exception {
    DestinationReapingBroker reapingBroker = new DestinationReapingBroker(mock(Broker.class), "queue.>", TimeUnit.SECONDS.toMillis(1));

    assertThat(reapingBroker.shouldBeReaped(new ActiveMQQueue("queue.one")), is(true));
    assertThat(reapingBroker.shouldBeReaped(new ActiveMQQueue("queue.two")), is(true));
    assertThat(reapingBroker.shouldBeReaped(new ActiveMQQueue("other.queue.too")), is(false));
    assertThat(reapingBroker.shouldBeReaped(new ActiveMQTopic("topic.one")), is(false));
    assertThat(reapingBroker.shouldBeReaped(new ActiveMQTopic("queue.but.not.really")), is(true));
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

  @Test
  public void timeoutAppliesPerDestination() throws Exception {
    BrokerService brokerService = createEmbeddedBrokerService();
    try {
      brokerService.setPlugins(new BrokerPlugin[]{new DestinationReaperPlugin("queue.>", TimeUnit.SECONDS.toMillis(2))});
      brokerService.start();
      Broker broker = brokerService.getBroker();

      addQueues(broker, "queue.gone");
      TimeUnit.SECONDS.sleep(1);
      addQueues(broker, "queue.present");
      TimeUnit.MILLISECONDS.sleep(1500);

      List<ActiveMQDestination> currentDestinations = Arrays.asList(broker.getDestinations());

      assertThat(currentDestinations, not(contains((ActiveMQDestination) new ActiveMQQueue("queue.gone"))));
      assertThat(currentDestinations, contains((ActiveMQDestination) new ActiveMQQueue("queue.present")));
    } finally {
      brokerService.stop();
    }
  }

}
