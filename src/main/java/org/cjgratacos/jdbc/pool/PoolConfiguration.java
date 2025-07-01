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
  /**
   * Gets the minimum pool size.
   *
   * @return the minimum pool size
   */
  public int getMinPoolSize() {
    return minPoolSize;
  }

  /**
   * Gets the maximum pool size.
   *
   * @return the maximum pool size
   */
  public int getMaxPoolSize() {
    return maxPoolSize;
  }

  /**
   * Gets the initial pool size.
   *
   * @return the initial pool size
   */
  public int getInitialPoolSize() {
    return initialPoolSize;
  }

  /**
   * Gets the connection timeout.
   *
   * @return the connection timeout
   */
  public Duration getConnectionTimeout() {
    return connectionTimeout;
  }

  /**
   * Gets the idle timeout.
   *
   * @return the idle timeout
   */
  public Duration getIdleTimeout() {
    return idleTimeout;
  }

  /**
   * Gets the maximum connection lifetime.
   *
   * @return the maximum lifetime
   */
  public Duration getMaxLifetime() {
    return maxLifetime;
  }

  /**
   * Gets the validation timeout.
   *
   * @return the validation timeout
   */
  public Duration getValidationTimeout() {
    return validationTimeout;
  }

  /**
   * Checks if connections are tested on borrow.
   *
   * @return true if test on borrow is enabled
   */
  public boolean isTestOnBorrow() {
    return testOnBorrow;
  }

  /**
   * Checks if connections are tested on return.
   *
   * @return true if test on return is enabled
   */
  public boolean isTestOnReturn() {
    return testOnReturn;
  }

  /**
   * Checks if idle connections are tested.
   *
   * @return true if test while idle is enabled
   */
  public boolean isTestWhileIdle() {
    return testWhileIdle;
  }

  /**
   * Gets the time between eviction runs.
   *
   * @return the time between eviction runs
   */
  public Duration getTimeBetweenEvictionRuns() {
    return timeBetweenEvictionRuns;
  }

  /**
   * Gets the number of tests per eviction run.
   *
   * @return the number of tests per eviction run
   */
  public int getNumTestsPerEvictionRun() {
    return numTestsPerEvictionRun;
  }

  /**
   * Checks if the pool blocks when exhausted.
   *
   * @return true if blocking when exhausted is enabled
   */
  public boolean isBlockWhenExhausted() {
    return blockWhenExhausted;
  }

  /**
   * Gets the maximum wait time for connections.
   *
   * @return the maximum wait time
   */
  public Duration getMaxWaitTime() {
    return maxWaitTime;
  }

  /**
   * Checks if the pool uses LIFO ordering.
   *
   * @return true if LIFO ordering is enabled
   */
  public boolean isLifo() {
    return lifo;
  }

  /**
   * Gets the connection properties.
   *
   * @return a copy of the connection properties
   */
  public Properties getConnectionProperties() {
    return new Properties(connectionProperties);
  }

  // Builder pattern for easier configuration
  /**
   * Builder for PoolConfiguration objects.
   */
  public static class Builder {
    private final PoolConfiguration config;

    /**
     * Creates a new builder with the given connection properties.
     *
     * @param connectionProperties the connection properties
     */
    public Builder(Properties connectionProperties) {
      this.config = new PoolConfiguration(connectionProperties);
    }

    /**
     * Sets the minimum pool size.
     *
     * @param size the minimum pool size
     * @return this builder
     */
    public Builder minPoolSize(int size) {
      config.minPoolSize = size;
      return this;
    }

    /**
     * Sets the maximum pool size.
     *
     * @param size the maximum pool size
     * @return this builder
     */
    public Builder maxPoolSize(int size) {
      config.maxPoolSize = size;
      return this;
    }

    /**
     * Sets the initial pool size.
     *
     * @param size the initial pool size
     * @return this builder
     */
    public Builder initialPoolSize(int size) {
      config.initialPoolSize = size;
      return this;
    }

    /**
     * Sets the connection timeout.
     *
     * @param timeout the connection timeout
     * @return this builder
     */
    public Builder connectionTimeout(Duration timeout) {
      config.connectionTimeout = timeout;
      return this;
    }

    /**
     * Sets the idle timeout.
     *
     * @param timeout the idle timeout
     * @return this builder
     */
    public Builder idleTimeout(Duration timeout) {
      config.idleTimeout = timeout;
      return this;
    }

    /**
     * Sets the maximum connection lifetime.
     *
     * @param lifetime the maximum lifetime
     * @return this builder
     */
    public Builder maxLifetime(Duration lifetime) {
      config.maxLifetime = lifetime;
      return this;
    }

    /**
     * Sets whether to test connections on borrow.
     *
     * @param test true to test on borrow
     * @return this builder
     */
    public Builder testOnBorrow(boolean test) {
      config.testOnBorrow = test;
      return this;
    }

    /**
     * Sets whether to test connections on return.
     *
     * @param test true to test on return
     * @return this builder
     */
    public Builder testOnReturn(boolean test) {
      config.testOnReturn = test;
      return this;
    }

    /**
     * Sets whether to test idle connections.
     *
     * @param test true to test while idle
     * @return this builder
     */
    public Builder testWhileIdle(boolean test) {
      config.testWhileIdle = test;
      return this;
    }

    /**
     * Sets whether to block when the pool is exhausted.
     *
     * @param block true to block when exhausted
     * @return this builder
     */
    public Builder blockWhenExhausted(boolean block) {
      config.blockWhenExhausted = block;
      return this;
    }

    /**
     * Sets the maximum wait time for connections.
     *
     * @param waitTime the maximum wait time
     * @return this builder
     */
    public Builder maxWaitTime(Duration waitTime) {
      config.maxWaitTime = waitTime;
      return this;
    }

    /**
     * Sets whether to use LIFO ordering.
     *
     * @param lifo true for LIFO ordering
     * @return this builder
     */
    public Builder lifo(boolean lifo) {
      config.lifo = lifo;
      return this;
    }

    /**
     * Builds the pool configuration.
     *
     * @return the configured PoolConfiguration
     */
    public PoolConfiguration build() {
      config.validateConfiguration();
      return config;
    }
  }
}