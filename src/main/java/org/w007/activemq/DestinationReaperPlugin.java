package org.w007.activemq;


import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.joda.time.format.ISOPeriodFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An ActiveMQ broker plugin that removes destinations matching
 * a pattern after a certain time has passed.
 * <p/>
 * Example of how to use this in the ActiveMQ Broker XML config file:
 * <p/>
 * <pre>
 *   &lt;plugins&gt;
 *     &lt;bean id="destinationReaperBrokerPlugin"&gt;
 *              class="org.w007.activemq.DestinationReaperPlugin"
 *              xmlns="http://www.springframework.org/schema/beans&gt;
 *       &lt;property name="destination"&gt;
 *         &lt;value&gt;staging.&gt;&lt;/value&gt;
 *       &lt;/property&gt;
 *       &lt;property name="destinationTimeToLive"&gt;
 *         &lt;value&gt;10000&lt;/value&gt;
 *       &lt;/property&gt;
 *     &lt;/bean&gt;
 *   &lt;/plugins&gt;
 * </pre>
 *
 * @org.apache.xbean.XBean element="destinationReaperBrokerPlugin"
 */
public class DestinationReaperPlugin implements BrokerPlugin {
  public static final Logger log = LoggerFactory.getLogger(DestinationReaperPlugin.class);
  private volatile Period destinationTimeToLive = new Period(1, PeriodType.days());
  private volatile String destination;

  public DestinationReaperPlugin() {
  }

  /**
   * For use in tests.
   */
  DestinationReaperPlugin(String destination, long destinationTimeToLive) {
    this.destination = destination;
    this.destinationTimeToLive = new Period(destinationTimeToLive, PeriodType.millis());
  }

  @Override
  public Broker installPlugin(Broker broker) throws Exception {
    log.info("Installing Destination Reaper broker plugin for destination {} and timeout {} ms",
        destination, destinationTimeToLive);
    return new DestinationReapingBroker(broker, destination, destinationTimeToLive.toStandardDuration().getMillis());
  }

  /**
   * After this time out period, the destination(s) matching {@link #getDestination()}
   * will be deleted.
   * <p/>
   * The default is one day.
   */
  public Period getDestinationTimeToLive() {
    return destinationTimeToLive;
  }

  /**
   * Set the period of time for which destinations matching {@link #getDestination()}
   * will live.
   *
   * @param destinationTimeToLive time in ISO8601 alternate extended format, Pyyyy-mm-ddThh:mm:ss
   */
  public void setDestinationTimeToLive(String destinationTimeToLive) {
    this.destinationTimeToLive = ISOPeriodFormat.alternateExtended().parsePeriod(destinationTimeToLive);
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
}
