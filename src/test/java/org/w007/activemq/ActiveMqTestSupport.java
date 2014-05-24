package org.w007.activemq;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.DestinationInfo;

import java.util.concurrent.atomic.AtomicInteger;

public class ActiveMqTestSupport {
  private static AtomicInteger brokerIndex = new AtomicInteger(0);

  protected void addQueues(Broker broker, String... names) throws Exception {
    for (String name : names) {
      ActiveMQQueue destination = new ActiveMQQueue(name);
      addDestination(broker, destination);
    }
  }

  protected void addTopics(Broker broker, String... names) throws Exception {
    for (String name : names) {
      ActiveMQTopic destination = new ActiveMQTopic(name);
      addDestination(broker, destination);
    }
  }

  protected void addDestination(Broker broker, ActiveMQDestination destination) throws Exception {
    DestinationInfo info = createAddDestinationInfo(broker, destination);
    broker.addDestinationInfo(broker.getAdminConnectionContext(), info);
  }

  protected DestinationInfo createAddDestinationInfo(Broker broker, ActiveMQDestination destination) {
    return new DestinationInfo(broker.getAdminConnectionContext().getConnectionId(),
        DestinationInfo.ADD_OPERATION_TYPE,
        destination);
  }

  protected Broker startEmbeddedBroker() throws Exception {
    BrokerService brokerService = createEmbeddedBrokerService();
    brokerService.start();
    return brokerService.getBroker();
  }

  protected BrokerService createEmbeddedBrokerService() throws Exception {
    return BrokerFactory.createBroker(getBrokerURI());
  }

  protected String getBrokerURI() {
    String brokerName = getClass().getCanonicalName() + brokerIndex.incrementAndGet();
    return "broker:(vm://" + brokerName + ")?advisorySupport=false&enableStatistics=false&persistent=false&brokerName=" + brokerName;
  }
}
