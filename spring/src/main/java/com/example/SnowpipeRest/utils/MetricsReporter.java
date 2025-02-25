package com.example.SnowpipeRest.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.ObjectName;

public class MetricsReporter {

  static final Logger LOGGER = LoggerFactory.getLogger(MetricsReporter.class);

  static MetricsReporter REPORTER;

  // Metrics Reporting
  private final ScheduledExecutorService scheduler;

  MetricsReporter() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
  }

  private void run() {
    scheduler.scheduleAtFixedRate(MetricsReporter::fetchAndPrintMetrics, 30, 30, TimeUnit.SECONDS);
  }

  public static synchronized void startReporter() {
    if (REPORTER == null) {
      REPORTER = new MetricsReporter();
      REPORTER.run();
      LOGGER.info("Started metrics repoter");
    }
  }

  private static void fetchAndPrintMetrics() {
    LOGGER.info("Fetching JMX metrics");
    try {
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      // Latency
      fetchAndPrintMetrics(
          mBeanServer,
          "snowflake.ingest.sdk:clientName=SNOWPIPE_REST_*,name=latency.*",
          "JMX latency: ");

      // Throughput
      fetchAndPrintMetrics(
          mBeanServer,
          "snowflake.ingest.sdk:clientName=SNOWPIPE_REST_*,name=throughput.*",
          "JMX throughput: ");

      // Blob
      fetchAndPrintMetrics(
          mBeanServer, "snowflake.ingest.sdk:clientName=SNOWPIPE_REST_*,name=blob.*", "JMX blob: ");

    } catch (Exception e) {
      LOGGER.error("Unable to log JMX metrics", e);
    }
  }

  private static void fetchAndPrintMetrics(
      MBeanServer mBeanServer, String queryNameString, String logPrefix) throws Exception {
    ObjectName queryName = new ObjectName(queryNameString);
    Set<ObjectName> names = mBeanServer.queryNames(queryName, null);
    if (names.isEmpty()) {
      LOGGER.info("No JMX metrics found: " + queryNameString);
    } else {
      for (ObjectName name : names) {
        MBeanInfo info = mBeanServer.getMBeanInfo(name);
        MBeanAttributeInfo[] attrInfo = info.getAttributes();
        Map<String, Object> attributes = new LinkedHashMap<>();
        for (MBeanAttributeInfo attr : attrInfo) {
          if (attr.isReadable())
            attributes.put(attr.getName(), mBeanServer.getAttribute(name, attr.getName()));
        }
        // Constructing the key-value pair string dynamically
        StringBuilder kvPairs = new StringBuilder();
        kvPairs.append("metric=" + name.getCanonicalName());
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
          if (kvPairs.length() > 0) {
            kvPairs.append(", ");
          }
          kvPairs.append(entry.getKey()).append("=").append(entry.getValue());
        }
        LOGGER.info(logPrefix + kvPairs);
      }
    }
  }
}
