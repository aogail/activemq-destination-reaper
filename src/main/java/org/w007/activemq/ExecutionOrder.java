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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ExecutionOrder that = (ExecutionOrder) o;

    if (!destination.equals(that.destination)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return destination.hashCode();
  }
}
