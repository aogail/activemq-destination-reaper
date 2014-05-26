activemq-destination-reaper
===========================

[![Build Status](https://travis-ci.org/aogail/activemq-destination-reaper.svg?branch=master)](https://travis-ci.org/aogail/activemq-destination-reaper)

ActiveMQ broker plugin that deletes destinations after a certain amount of time.

Example XML configuration within a &lt;broker> tag:

    <plugins>
      <bean id="destinationReaperBrokerPlugin"
            class="org.w007.activemq.DestinationReaperPlugin"
            xmlns="http://www.springframework.org/schema/beans">
        <property name="destination">
          <value>these.will.die.after.ten.minutes.></value>
        </property>
        <property name="destinationTimeToLive">
          <value>P0000-00-00T00:10:00</value>
        </property>
      </bean>
    </plugins>

