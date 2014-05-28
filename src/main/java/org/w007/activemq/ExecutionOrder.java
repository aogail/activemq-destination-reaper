package org.w007.activemq;

import org.apache.activemq.command.ActiveMQDestination;

class ExecutionOrder {
  final ActiveMQDestination destination;
  final long timeToDieMillis;

  public ExecutionOrder(ActiveMQDestination destination, long timeToDieMillis) {
    this.destination = destination;
    this.timeToDieMillis = timeToDieMillis;
  }

  @Override
  public String toString() {
    return String.format("ExecutionOrder{destination=%s, timeToDieMillis=%d}", destination, timeToDieMillis);
  }
}
