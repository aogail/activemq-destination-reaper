package org.w007.activemq;

import org.apache.activemq.broker.Broker;
import org.joda.time.Period;
import org.joda.time.format.ISOPeriodFormat;
import org.joda.time.format.PeriodFormatter;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

public class DestinationReaperPluginTest extends ActiveMqTestSupport {


  @Test
  public void canInstantiatePlugin() {
    new DestinationReaperPlugin();
  }

  @Test
  public void installPluginReturnsDifferentBrokerInstance() throws Exception {
    Broker broker = mock(Broker.class);
    DestinationReaperPlugin plugin = new DestinationReaperPlugin("d", 1);
    assertThat(plugin.installPlugin(broker), is(not(broker)));
  }

  @Test
  public void canSetTimeout() {
    DestinationReaperPlugin plugin = new DestinationReaperPlugin();
    PeriodFormatter fmt = ISOPeriodFormat.alternateExtended();
    Period timeout = fmt.parsePeriod("P0000-00-00T00:00:10");
    plugin.setDestinationTimeToLive(fmt.print(timeout));
    assertThat(plugin.getDestinationTimeToLive(), equalTo(timeout));
    assertThat(plugin.getDestinationTimeToLive().toStandardDuration().getMillis(), equalTo(TimeUnit.SECONDS.toMillis(10)));
    timeout = fmt.parsePeriod("P0000-00-00T00:00:03");
    plugin.setDestinationTimeToLive(fmt.print(timeout));
    assertThat(plugin.getDestinationTimeToLive(), equalTo(timeout));
    assertThat(plugin.getDestinationTimeToLive().toStandardDuration().getMillis(), equalTo(TimeUnit.SECONDS.toMillis(3)));
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
  public void setsCorrectValuesOnDestinationReaperBroker() throws Exception {
    DestinationReaperPlugin plugin = new DestinationReaperPlugin();
    plugin.setDestination("foo");
    long timeoutSeconds = 10;
    long timeoutMillis = TimeUnit.SECONDS.toMillis(timeoutSeconds);
    plugin.setDestinationTimeToLive(String.format("P0000-00-00T00:00:%d", timeoutSeconds));
    DestinationReapingBroker broker = (DestinationReapingBroker) plugin.installPlugin(mock(Broker.class));

    assertThat(broker.getDestinationTimeToLiveMillis(), is(timeoutMillis));
  }
}
