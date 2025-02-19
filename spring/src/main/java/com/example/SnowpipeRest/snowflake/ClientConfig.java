package com.example.SnowpipeRest.snowflake;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ClientConfig {

  static final Logger LOGGER = LoggerFactory.getLogger(ClientConfig.class);

  @Value("${snowflake.url}")
  private String snowflakeUrl;

  @Value("${snowflake.user}")
  private String snowflakeUser;

  @Value("${snowflake.role}")
  private String snowflakeRole;

  @Value("${snowflake.private_key}")
  private String snowflakePrivateKey;

  @Value("${rest_api.max_client_lag}")
  private String maxClientLag;

  public String getSnowflakeUrl() {
    if (snowflakeUrl == null) {
      LOGGER.info("Defaulting to SNOWFLAKE_URL environment variable");
      return System.getenv("SNOWFLAKE_URL");
    }
    return snowflakeUrl;
  }

  public String getSnowflakeUser() {
    if (snowflakeUser == null) {
      LOGGER.info("Defaulting to SNOWFLAKE_USER environment variable");
      return System.getenv("SNOWFLAKE_USER");
    }
    return snowflakeUser;
  }

  public String getSnowflakeRole() {
    if (snowflakeRole == null) {
      LOGGER.info("Defaulting to SNOWFLAKE_ROLE environment variable");
      return System.getenv("SNOWFLAKE_ROLE");
    }
    return snowflakeRole;
  }

  public String getSnowflakePrivateKey() {
    if (snowflakePrivateKey == null) {
      LOGGER.info("Defaulting to SNOWFLAKE_PRIVATE_KEY environment variable");
      return System.getenv("SNOWFLAKE_PRIVATE_KEY");
    }
    return snowflakePrivateKey;
  }

  public String getMaxClientLag() {
    if (maxClientLag == null) {
      LOGGER.info("Defaulting to REST_API_MAX_CLIENT_LAG environment variable");
      return System.getenv("REST_API_MAX_CLIENT_LAG");
    }
    return maxClientLag;
  }
}
