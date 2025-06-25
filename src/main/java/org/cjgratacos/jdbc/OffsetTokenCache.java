package org.cjgratacos.jdbc;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache for DynamoDB NextToken values at specific offset positions.
 *
 * <p>This cache helps optimize pagination with OFFSET by storing NextToken values at regular
 * intervals. When a query requests a large OFFSET, instead of fetching and discarding all preceding
 * rows, we can start from the nearest cached token position.
 *
 * <h2>Features:</h2>
 *
 * <ul>
 *   <li>LRU eviction policy with configurable size
 *   <li>TTL support for cache entries
 *   <li>Thread-safe implementation
 *   <li>Per-query caching (keyed by query pattern)
 * </ul>
 *
 * <h2>Example:</h2>
 *
 * <pre>{@code
 * // Cache tokens every 100 rows
 * OffsetTokenCache cache = new OffsetTokenCache(100, 100, 3600);
 *
 * // Store a token at offset 300
 * cache.put("SELECT * FROM users", 300, "token-at-300");
 *
 * // Retrieve nearest token for offset 350
 * TokenEntry entry = cache.getNearestToken("SELECT * FROM users", 350);
 * // Returns token at offset 300
 * }</pre>
 *
 * @author CJ Gratacos
 * @since 1.0
 */
public class OffsetTokenCache {

  private static final Logger logger = LoggerFactory.getLogger(OffsetTokenCache.class);

  /** Represents a cached token entry with offset position and timestamp. */
  public static class TokenEntry {
    private final int offset;
    private final String token;
    private final Instant timestamp;

    /**
     * Creates a new token entry with the current timestamp.
     *
     * @param offset the offset position for this token
     * @param token the DynamoDB pagination token
     */
    public TokenEntry(int offset, String token) {
      this.offset = offset;
      this.token = token;
      this.timestamp = Instant.now();
    }

    /**
     * Gets the offset position associated with this token.
     *
     * @return the offset position
     */
    public int getOffset() {
      return offset;
    }

    /**
     * Gets the DynamoDB pagination token.
     *
     * @return the pagination token
     */
    public String getToken() {
      return token;
    }

    /**
     * Gets the timestamp when this token entry was created.
     *
     * @return the creation timestamp
     */
    public Instant getTimestamp() {
      return timestamp;
    }

    /**
     * Checks if this token entry has expired based on the TTL.
     *
     * @param ttlSeconds the time-to-live in seconds (0 or negative means no expiration)
     * @return true if the entry has expired, false otherwise
     */
    public boolean isExpired(long ttlSeconds) {
      if (ttlSeconds <= 0) {
        return false; // No expiration
      }
      return Instant.now().isAfter(timestamp.plusSeconds(ttlSeconds));
    }
  }

  /** Cache key combining query pattern and offset. */
  private static class CacheKey {
    private final String queryPattern;
    private final int offset;

    public CacheKey(String queryPattern, int offset) {
      this.queryPattern = normalizeQuery(queryPattern);
      this.offset = offset;
    }

    private static String normalizeQuery(String query) {
      // Remove whitespace variations and convert to uppercase for consistent keys
      return query.replaceAll("\\s+", " ").toUpperCase().trim();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CacheKey cacheKey = (CacheKey) o;
      return offset == cacheKey.offset && Objects.equals(queryPattern, cacheKey.queryPattern);
    }

    @Override
    public int hashCode() {
      return Objects.hash(queryPattern, offset);
    }
  }

  private final int maxSize;
  private final int cacheInterval;
  private final long ttlSeconds;
  private final Map<String, LinkedHashMap<Integer, TokenEntry>> queryCache;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * Creates a new offset token cache.
   *
   * @param maxSize the maximum number of entries per query pattern
   * @param cacheInterval the interval at which to cache tokens (e.g., every 100 rows)
   * @param ttlSeconds the time-to-live for cache entries in seconds (0 for no expiration)
   */
  public OffsetTokenCache(int maxSize, int cacheInterval, long ttlSeconds) {
    if (maxSize <= 0) {
      throw new IllegalArgumentException("Cache size must be positive");
    }
    if (cacheInterval <= 0) {
      throw new IllegalArgumentException("Cache interval must be positive");
    }
    if (ttlSeconds < 0) {
      throw new IllegalArgumentException("TTL must be positive");
    }

    this.maxSize = maxSize;
    this.cacheInterval = cacheInterval;
    this.ttlSeconds = ttlSeconds;
    this.queryCache = new ConcurrentHashMap<>();

    if (logger.isInfoEnabled()) {
      logger.info(
          "Initialized OffsetTokenCache with maxSize={}, interval={}, ttl={}s",
          maxSize,
          cacheInterval,
          ttlSeconds);
    }
  }

  /**
   * Stores a token for the given query pattern and offset.
   *
   * @param queryPattern the SQL query pattern
   * @param offset the offset position
   * @param token the NextToken value from DynamoDB
   */
  public void put(String queryPattern, int offset, String token) {
    if (token == null || token.isEmpty()) {
      return; // Don't cache empty tokens
    }

    // Only cache at intervals
    if (offset % cacheInterval != 0) {
      return;
    }

    lock.writeLock().lock();
    try {
      String normalizedQuery = CacheKey.normalizeQuery(queryPattern);
      LinkedHashMap<Integer, TokenEntry> offsetMap =
          queryCache.computeIfAbsent(
              normalizedQuery,
              k ->
                  new LinkedHashMap<Integer, TokenEntry>(16, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<Integer, TokenEntry> eldest) {
                      return size() > maxSize;
                    }
                  });

      offsetMap.put(offset, new TokenEntry(offset, token));

      if (logger.isDebugEnabled()) {
        logger.debug("Cached token for query pattern at offset {}", offset);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Retrieves the nearest cached token for the given offset.
   *
   * @param queryPattern the SQL query pattern
   * @param targetOffset the desired offset
   * @return the nearest token entry with offset less than or equal to targetOffset, or null if none
   *     found
   */
  public TokenEntry getNearestToken(String queryPattern, int targetOffset) {
    lock.readLock().lock();
    try {
      String normalizedQuery = CacheKey.normalizeQuery(queryPattern);
      LinkedHashMap<Integer, TokenEntry> offsetMap = queryCache.get(normalizedQuery);

      if (offsetMap == null || offsetMap.isEmpty()) {
        return null;
      }

      TokenEntry nearest = null;
      for (Map.Entry<Integer, TokenEntry> entry : offsetMap.entrySet()) {
        TokenEntry tokenEntry = entry.getValue();

        // Skip expired entries
        if (tokenEntry.isExpired(ttlSeconds)) {
          continue;
        }

        // Find the largest offset that's still <= targetOffset
        if (tokenEntry.getOffset() <= targetOffset) {
          if (nearest == null || tokenEntry.getOffset() > nearest.getOffset()) {
            nearest = tokenEntry;
          }
        }
      }

      if (nearest != null && logger.isDebugEnabled()) {
        logger.debug(
            "Found cached token at offset {} for target offset {}",
            nearest.getOffset(),
            targetOffset);
      }

      return nearest;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Clears all cache entries for the given query pattern.
   *
   * @param queryPattern the SQL query pattern to clear
   */
  public void clearQuery(String queryPattern) {
    lock.writeLock().lock();
    try {
      String normalizedQuery = CacheKey.normalizeQuery(queryPattern);
      queryCache.remove(normalizedQuery);

      if (logger.isDebugEnabled()) {
        logger.debug("Cleared cache for query pattern");
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** Clears all cache entries. */
  public void clearAll() {
    lock.writeLock().lock();
    try {
      queryCache.clear();
      logger.info("Cleared entire offset token cache");
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Gets the current size of the cache.
   *
   * @return the total number of cached entries across all query patterns
   */
  public int size() {
    lock.readLock().lock();
    try {
      return queryCache.values().stream().mapToInt(Map::size).sum();
    } finally {
      lock.readLock().unlock();
    }
  }

  /** Removes expired entries from the cache. */
  public void evictExpired() {
    if (ttlSeconds <= 0) {
      return; // No expiration configured
    }

    lock.writeLock().lock();
    try {
      queryCache
          .values()
          .forEach(
              offsetMap ->
                  offsetMap.entrySet().removeIf(entry -> entry.getValue().isExpired(ttlSeconds)));

      // Remove empty query entries
      queryCache.entrySet().removeIf(entry -> entry.getValue().isEmpty());

      if (logger.isDebugEnabled()) {
        logger.debug("Evicted expired entries from cache");
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Checks if caching should be performed for the given offset.
   *
   * @param offset the offset to check
   * @return true if the offset should be cached
   */
  public boolean shouldCache(int offset) {
    return offset > 0 && offset % cacheInterval == 0;
  }

  /**
   * Gets cache statistics for monitoring.
   *
   * @return a map of statistics
   */
  public Map<String, Object> getStats() {
    lock.readLock().lock();
    try {
      Map<String, Object> stats = new LinkedHashMap<>();
      stats.put("totalQueries", queryCache.size());
      stats.put("totalEntries", size());
      stats.put("maxSize", maxSize);
      stats.put("cacheInterval", cacheInterval);
      stats.put("ttlSeconds", ttlSeconds);
      return stats;
    } finally {
      lock.readLock().unlock();
    }
  }
}
