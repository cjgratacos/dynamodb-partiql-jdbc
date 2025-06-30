package org.cjgratacos.jdbc.pool;

import java.time.Duration;
import java.util.Properties;

/**
 * Configuration for DynamoDB connection pool.
 *
 * <p>This class holds all configuration parameters for the connection pool including size limits,
 * timeouts, validation settings, and other pool behavior options.
 *
 * @author CJ Gratacos
 * @since 1.0
 */
public class PoolConfiguration {

  // Pool size configuration
  private int minPoolSize = 5;
  private int maxPoolSize = 20;
  private int initialPoolSize = 5;

  // Timeout configuration
  private Duration connectionTimeout = Duration.ofSeconds(30);
  private Duration idleTimeout = Duration.ofMinutes(10);
  private Duration maxLifetime = Duration.ofMinutes(30);
  private Duration validationTimeout = Duration.ofSeconds(5);

  // Validation configuration
  private boolean testOnBorrow = true;
  private boolean testOnReturn = false;
  private boolean testWhileIdle = true;
  private Duration timeBetweenEvictionRuns = Duration.ofMinutes(1);
  private int numTestsPerEvictionRun = 3;

  // Pool behavior
  private boolean blockWhenExhausted = true;
  private Duration maxWaitTime = Duration.ofSeconds(30);
  private boolean lifo = true; // Last-In-First-Out vs First-In-First-Out

  // Connection properties
  private final Properties connectionProperties;

  /**
   * Creates a new pool configuration with the given connection properties.
   *
   * @param connectionProperties properties for creating new connections
   */
  public PoolConfiguration(Properties connectionProperties) {
    this.connectionProperties = new Properties();
    if (connectionProperties != null) {
      this.connectionProperties.putAll(connectionProperties);
    }
    loadFromProperties();
  }

  /**
   * Loads pool configuration from connection properties.
   */
  private void loadFromProperties() {
    // Pool size
    minPoolSize = getIntProperty("pool.minSize", minPoolSize);
    maxPoolSize = getIntProperty("pool.maxSize", maxPoolSize);
    initialPoolSize = getIntProperty("pool.initialSize", initialPoolSize);

    // Timeouts
    connectionTimeout = getDurationProperty("pool.connectionTimeout", connectionTimeout);
    idleTimeout = getDurationProperty("pool.idleTimeout", idleTimeout);
    maxLifetime = getDurationProperty("pool.maxLifetime", maxLifetime);
    validationTimeout = getDurationProperty("pool.validationTimeout", validationTimeout);

    // Validation
    testOnBorrow = getBooleanProperty("pool.testOnBorrow", testOnBorrow);
    testOnReturn = getBooleanProperty("pool.testOnReturn", testOnReturn);
    testWhileIdle = getBooleanProperty("pool.testWhileIdle", testWhileIdle);
    timeBetweenEvictionRuns =
        getDurationProperty("pool.timeBetweenEvictionRuns", timeBetweenEvictionRuns);
    numTestsPerEvictionRun = getIntProperty("pool.numTestsPerEvictionRun", numTestsPerEvictionRun);

    // Pool behavior
    blockWhenExhausted = getBooleanProperty("pool.blockWhenExhausted", blockWhenExhausted);
    maxWaitTime = getDurationProperty("pool.maxWaitTime", maxWaitTime);
    lifo = getBooleanProperty("pool.lifo", lifo);

    // Validate configuration
    validateConfiguration();
  }

  private void validateConfiguration() {
    if (minPoolSize < 0) {
      throw new IllegalArgumentException("minPoolSize must be >= 0");
    }
    if (maxPoolSize < 1) {
      throw new IllegalArgumentException("maxPoolSize must be >= 1");
    }
    if (minPoolSize > maxPoolSize) {
      throw new IllegalArgumentException("minPoolSize must be <= maxPoolSize");
    }
    if (initialPoolSize < minPoolSize || initialPoolSize > maxPoolSize) {
      throw new IllegalArgumentException(
          "initialPoolSize must be between minPoolSize and maxPoolSize");
    }
  }

  private int getIntProperty(String key, int defaultValue) {
    String value = connectionProperties.getProperty(key);
    if (value != null) {
      try {
        return Integer.parseInt(value);
      } catch (NumberFormatException e) {
        // Log warning and use default
        return defaultValue;
      }
    }
    return defaultValue;
  }

  private boolean getBooleanProperty(String key, boolean defaultValue) {
    String value = connectionProperties.getProperty(key);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return defaultValue;
  }

  private Duration getDurationProperty(String key, Duration defaultValue) {
    String value = connectionProperties.getProperty(key);
    if (value != null) {
      try {
        // Support seconds as default unit
        long seconds = Long.parseLong(value);
        return Duration.ofSeconds(seconds);
      } catch (NumberFormatException e) {
        // Try parsing as ISO-8601 duration
        try {
          return Duration.parse(value);
        } catch (Exception ex) {
          return defaultValue;
        }
      }
    }
    return defaultValue;
  }

  // Getters
  public int getMinPoolSize() {
    return minPoolSize;
  }

  public int getMaxPoolSize() {
    return maxPoolSize;
  }

  public int getInitialPoolSize() {
    return initialPoolSize;
  }

  public Duration getConnectionTimeout() {
    return connectionTimeout;
  }

  public Duration getIdleTimeout() {
    return idleTimeout;
  }

  public Duration getMaxLifetime() {
    return maxLifetime;
  }

  public Duration getValidationTimeout() {
    return validationTimeout;
  }

  public boolean isTestOnBorrow() {
    return testOnBorrow;
  }

  public boolean isTestOnReturn() {
    return testOnReturn;
  }

  public boolean isTestWhileIdle() {
    return testWhileIdle;
  }

  public Duration getTimeBetweenEvictionRuns() {
    return timeBetweenEvictionRuns;
  }

  public int getNumTestsPerEvictionRun() {
    return numTestsPerEvictionRun;
  }

  public boolean isBlockWhenExhausted() {
    return blockWhenExhausted;
  }

  public Duration getMaxWaitTime() {
    return maxWaitTime;
  }

  public boolean isLifo() {
    return lifo;
  }

  public Properties getConnectionProperties() {
    return new Properties(connectionProperties);
  }

  // Builder pattern for easier configuration
  public static class Builder {
    private final PoolConfiguration config;

    public Builder(Properties connectionProperties) {
      this.config = new PoolConfiguration(connectionProperties);
    }

    public Builder minPoolSize(int size) {
      config.minPoolSize = size;
      return this;
    }

    public Builder maxPoolSize(int size) {
      config.maxPoolSize = size;
      return this;
    }

    public Builder initialPoolSize(int size) {
      config.initialPoolSize = size;
      return this;
    }

    public Builder connectionTimeout(Duration timeout) {
      config.connectionTimeout = timeout;
      return this;
    }

    public Builder idleTimeout(Duration timeout) {
      config.idleTimeout = timeout;
      return this;
    }

    public Builder maxLifetime(Duration lifetime) {
      config.maxLifetime = lifetime;
      return this;
    }

    public Builder testOnBorrow(boolean test) {
      config.testOnBorrow = test;
      return this;
    }

    public Builder testOnReturn(boolean test) {
      config.testOnReturn = test;
      return this;
    }

    public Builder testWhileIdle(boolean test) {
      config.testWhileIdle = test;
      return this;
    }

    public Builder blockWhenExhausted(boolean block) {
      config.blockWhenExhausted = block;
      return this;
    }

    public Builder maxWaitTime(Duration waitTime) {
      config.maxWaitTime = waitTime;
      return this;
    }

    public Builder lifo(boolean lifo) {
      config.lifo = lifo;
      return this;
    }

    public PoolConfiguration build() {
      config.validateConfiguration();
      return config;
    }
  }
}