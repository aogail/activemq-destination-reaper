package org.w007.activemq;

import org.apache.activemq.broker.Broker;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

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
    long timeoutMillis = TimeUnit.SECONDS.toMillis(1);
    plugin.setDestinationTimeToLive(timeoutMillis);
    assertThat(plugin.getDestinationTimeToLive(), is(timeoutMillis));
    timeoutMillis = TimeUnit.SECONDS.toMillis(3);
    plugin.setDestinationTimeToLive(timeoutMillis);
    assertThat(plugin.getDestinationTimeToLive(), is(timeoutMillis));
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

}
