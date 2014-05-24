package org.w007.activemq;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.DestinationInfo;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;

public class DestinationReaperPluginTest {

  private static AtomicInteger brokerIndex = new AtomicInteger(0);

  @Test
  public void canInstantiatePlugin() {
    new DestinationReaperPlugin();
  }

  @Test
  public void installPluginReturnsBrokerInstance() throws Exception {
    Broker broker = mock(Broker.class);
    DestinationReaperPlugin plugin = new DestinationReaperPlugin();
    assertThat(plugin.installPlugin(broker), is(broker));
  }

  @Test
  public void canSetTimeout() {
    DestinationReaperPlugin plugin = new DestinationReaperPlugin();
    long timeoutMillis = TimeUnit.SECONDS.toMillis(1);
    plugin.setTimeoutMillis(timeoutMillis);
    assertThat(plugin.getTimeoutMillis(), is(timeoutMillis));
    timeoutMillis = TimeUnit.SECONDS.toMillis(3);
    plugin.setTimeoutMillis(timeoutMillis);
    assertThat(plugin.getTimeoutMillis(), is(timeoutMillis));
  }

  @Test
  public void canSetPattern() {
    DestinationReaperPlugin plugin = new DestinationReaperPlugin();
    String pattern = "these.will.die.>";
    plugin.setDestination(pattern);
    assertThat(plugin.getDestination(), is(pattern));
    pattern = "others.will.die.>";
    plugin.setDestination(pattern);
    assertThat(plugin.getDestination(), is(pattern));
  }

  @Test
  public void findsMatchingDestinations() throws Exception {
    Broker broker = startEmbeddedBroker();
    try {
      addQueues(broker, "queue.one", "queue.two", "other.queue.one");
      addTopics(broker, "topic.one", "topic.two", "other.topic.one");

      DestinationReaperPlugin plugin = new DestinationReaperPlugin();
      plugin.setDestination("queue.>");
      plugin.installPlugin(broker);

      assertThat(plugin.getMatchingDestinations(),
          containsInAnyOrder((ActiveMQDestination) new ActiveMQQueue("queue.one"), new ActiveMQQueue("queue.two")));

      plugin.setDestination("topic.>");
      plugin.installPlugin(broker);

      assertThat(plugin.getMatchingDestinations(),
          containsInAnyOrder((ActiveMQDestination) new ActiveMQTopic("topic.one"), new ActiveMQTopic("topic.two")));
    } finally {
      broker.stop();
    }
  }

  @Test
  public void removesDestinationsAfterTimeoutPasses() throws Exception {
    Broker broker = startEmbeddedBroker();
    try {
      addQueues(broker, "queue.one", "queue.two", "other.queue.one");
      addTopics(broker, "topic.one", "topic.two", "other.topic.one");
      ActiveMQDestination[] originalDestinations = broker.getDestinations();

      DestinationReaperPlugin plugin = new DestinationReaperPlugin();
      plugin.setDestination("queue.>");
      plugin.setTimeoutMillis(TimeUnit.SECONDS.toMillis(1));
      plugin.installPlugin(broker);

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
      broker.stop();
    }
  }

  private void addQueues(Broker broker, String... names) throws Exception {
    for (String name : names) {
      ActiveMQQueue destination = new ActiveMQQueue(name);
      addDestination(broker, destination);
    }
  }

  private void addTopics(Broker broker, String... names) throws Exception {
    for (String name : names) {
      ActiveMQTopic destination = new ActiveMQTopic(name);
      addDestination(broker, destination);
    }
  }

  private void addDestination(Broker broker, ActiveMQDestination destination) throws Exception {
    DestinationInfo info = createAddDestinationInfo(broker, destination);
    broker.addDestinationInfo(broker.getAdminConnectionContext(), info);
  }

  private DestinationInfo createAddDestinationInfo(Broker broker, ActiveMQDestination destination) {
    return new DestinationInfo(broker.getAdminConnectionContext().getConnectionId(),
        DestinationInfo.ADD_OPERATION_TYPE,
        destination);
  }

  private Broker startEmbeddedBroker() throws Exception {
    BrokerService brokerService = BrokerFactory.createBroker(getBrokerURI());
    brokerService.start();
    return brokerService.getBroker();
  }

  private String getBrokerURI() {
    String brokerName = getClass().getCanonicalName() + brokerIndex.incrementAndGet();
    return "broker:(vm://" + brokerName + ")?advisorySupport=false&enableStatistics=false&persistent=false&brokerName=" + brokerName;
  }
}
