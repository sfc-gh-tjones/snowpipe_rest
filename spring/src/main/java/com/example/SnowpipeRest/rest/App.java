package com.example.SnowpipeRest.rest;

import com.example.SnowpipeRest.snowflake.ChannelManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PreDestroy;
import java.util.Collections;

/** Entrypoint for the Snowpipe Rest Application */
@SpringBootApplication
public class App {

  static final Logger LOGGER = LoggerFactory.getLogger(App.class);

  public static void main(String[] args) {
    SpringApplication app = new SpringApplication(App.class);
    if (!ChannelManager.getInstance().credentialsValid()) {
      throw new RuntimeException("Unable to start server - credentials to Snowflake are not valid");
    }
    app.setDefaultProperties(Collections.singletonMap("server.port", 8080));
    app.run(args);
  }

  @PreDestroy
  public void onExit() {
    LOGGER.info("Shutting down server");
    if (Resource.ingestEngine != null) {
      Resource.ingestEngine.shutDown();
    }
  }
}
