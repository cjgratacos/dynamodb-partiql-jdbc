package org.cjgratacos.jdbc;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe cached schema entry with TTL support and concurrent access optimization.
 *
 * <p>This class represents a cached schema entry for DynamoDB tables with built-in TTL management,
 * concurrent access control, and refresh coordination. It supports both basic schema information
 * and enhanced column metadata with proper expiration handling.
 *
 * <h2>Features:</h2>
 *
 * <ul>
 *   <li>Thread-safe read/write operations with ReentrantReadWriteLock
 *   <li>TTL-based expiration with configurable lifetime
 *   <li>Refresh coordination to prevent duplicate schema detection
 *   <li>Support for both basic and enhanced schema data
 *   <li>Access tracking for cache hit/miss statistics
 * </ul>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 * @param <T> the type of schema data stored (Integer for basic types, ColumnMetadata for enhanced)
 */
public class CachedSchemaEntry<T> {

  private final String tableName;
  private final long createdAt;
  private final long ttlMs;
  private final ReentrantReadWriteLock lock;
  private final AtomicBoolean refreshing;

  private volatile Map<String, T> schemaData;
  private volatile long lastAccessTime;
  private volatile long refreshCount;
  private volatile boolean valid;

  /**
   * Creates a new cached schema entry.
   *
   * @param tableName the name of the DynamoDB table
   * @param schemaData the schema data to cache
   * @param ttlMs the time-to-live in milliseconds
   */
  public CachedSchemaEntry(
      final String tableName, final Map<String, T> schemaData, final long ttlMs) {
    this.tableName = tableName;
    this.schemaData = schemaData;
    this.ttlMs = ttlMs;
    this.createdAt = System.currentTimeMillis();
    this.lastAccessTime = this.createdAt;
    this.lock = new ReentrantReadWriteLock();
    this.refreshing = new AtomicBoolean(false);
    this.refreshCount = 0;
    this.valid = true;
  }

  /**
   * Gets the cached schema data if not expired.
   *
   * @return the schema data, or null if expired
   */
  public Map<String, T> getSchemaData() {
    this.lock.readLock().lock();
    try {
      if (!this.isValid()) {
        return null;
      }

      this.lastAccessTime = System.currentTimeMillis();
      return this.schemaData;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /**
   * Updates the cached schema data.
   *
   * @param newSchemaData the new schema data
   * @return true if update was successful, false if entry is invalid
   */
  public boolean updateSchemaData(final Map<String, T> newSchemaData) {
    this.lock.writeLock().lock();
    try {
      if (!this.valid) {
        return false;
      }

      this.schemaData = newSchemaData;
      this.lastAccessTime = System.currentTimeMillis();
      this.refreshCount++;
      return true;
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /**
   * Checks if this cache entry is still valid (not expired).
   *
   * @return true if valid, false if expired
   */
  public boolean isValid() {
    if (!this.valid) {
      return false;
    }

    // Zero or negative TTL means always invalid
    if (this.ttlMs <= 0) {
      this.valid = false;
      return false;
    }

    final var currentTime = System.currentTimeMillis();
    final var age = currentTime - this.createdAt;

    if (age > this.ttlMs) {
      this.valid = false;
      return false;
    }

    return true;
  }

  /**
   * Marks this entry as refreshing to coordinate concurrent refresh attempts.
   *
   * @return true if this thread should perform the refresh, false if another thread is already
   *     refreshing
   */
  public boolean markRefreshing() {
    return this.refreshing.compareAndSet(false, true);
  }

  /** Marks the refresh as complete. */
  public void markRefreshComplete() {
    this.refreshing.set(false);
  }

  /**
   * Checks if this entry is currently being refreshed.
   *
   * @return true if being refreshed, false otherwise
   */
  public boolean isRefreshing() {
    return this.refreshing.get();
  }

  /** Invalidates this cache entry. */
  public void invalidate() {
    this.lock.writeLock().lock();
    try {
      this.valid = false;
      this.schemaData = null;
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /**
   * Gets the age of this cache entry in milliseconds.
   *
   * @return the age in milliseconds
   */
  public long getAgeMs() {
    return System.currentTimeMillis() - this.createdAt;
  }

  /**
   * Gets the time since last access in milliseconds.
   *
   * @return the time since last access in milliseconds
   */
  public long getTimeSinceLastAccessMs() {
    return System.currentTimeMillis() - this.lastAccessTime;
  }

  /**
   * Gets the number of times this entry has been refreshed.
   *
   * @return the refresh count
   */
  public long getRefreshCount() {
    return this.refreshCount;
  }

  /**
   * Gets the table name for this cache entry.
   *
   * @return the table name
   */
  public String getTableName() {
    return this.tableName;
  }

  /**
   * Gets the TTL for this cache entry.
   *
   * @return the TTL in milliseconds
   */
  public long getTtlMs() {
    return this.ttlMs;
  }

  /**
   * Gets the creation timestamp.
   *
   * @return the creation time in milliseconds since epoch
   */
  public long getCreatedAt() {
    return this.createdAt;
  }

  /**
   * Gets the last access timestamp.
   *
   * @return the last access time in milliseconds since epoch
   */
  public long getLastAccessTime() {
    return this.lastAccessTime;
  }

  /**
   * Gets cache entry statistics.
   *
   * @return a map containing entry statistics
   */
  public Map<String, Object> getStatistics() {
    this.lock.readLock().lock();
    try {
      return Map.of(
          "tableName",
          this.tableName,
          "valid",
          this.valid,
          "ageMs",
          this.getAgeMs(),
          "timeSinceLastAccessMs",
          this.getTimeSinceLastAccessMs(),
          "refreshCount",
          this.refreshCount,
          "refreshing",
          this.refreshing.get(),
          "ttlMs",
          this.ttlMs,
          "schemaAttributeCount",
          this.schemaData != null ? this.schemaData.size() : 0);
    } finally {
      this.lock.readLock().unlock();
    }
  }

  @Override
  public String toString() {
    return String.format(
        "CachedSchemaEntry{table=%s, valid=%s, age=%dms, attributes=%d, refreshCount=%d}",
        this.tableName,
        this.valid,
        this.getAgeMs(),
        this.schemaData != null ? this.schemaData.size() : 0,
        this.refreshCount);
  }
}
