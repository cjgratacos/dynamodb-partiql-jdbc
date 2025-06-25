package org.cjgratacos.jdbc;

import org.junit.jupiter.api.Test;

public class OffsetTokenCacheDebugTest {

  @Test
  void debugLruBehavior() {
    OffsetTokenCache cache = new OffsetTokenCache(5, 10, 60);
    String query = "SELECT * FROM users";

    System.out.println("=== Debug LRU Behavior ===");

    // Fill cache to capacity
    for (int i = 1; i <= 5; i++) {
      cache.put(query, i * 10, "token" + i);
      System.out.println("Put offset " + (i * 10) + " with token" + i);
    }

    System.out.println("\nCache after filling (should have 10-50):");
    for (int i = 10; i <= 60; i += 10) {
      OffsetTokenCache.TokenEntry e = cache.getNearestToken(query, i);
      System.out.println("Offset " + i + ": " + (e != null ? "Found at " + e.getOffset() : "NULL"));
    }

    // Access offset 10
    System.out.println("\nAccessing offset 10...");
    cache.getNearestToken(query, 10);

    // Add new entry
    System.out.println("\nAdding offset 60...");
    cache.put(query, 60, "token6");

    System.out.println("\nCache after adding offset 60:");
    for (int i = 10; i <= 60; i += 10) {
      OffsetTokenCache.TokenEntry e = cache.getNearestToken(query, i);
      System.out.println("Offset " + i + ": " + (e != null ? "Found at " + e.getOffset() : "NULL"));
    }
  }

  @Test
  void debugCacheBehavior() {
    OffsetTokenCache cache = new OffsetTokenCache(5, 10, 60);

    // Test what gets cached
    String query = "SELECT * FROM users";

    System.out.println("=== Debug Cache Behavior ===");
    for (int i = 0; i <= 60; i += 10) {
      String token = "token_" + i;
      cache.put(query, i, token);
      System.out.println("Put offset " + i + " with token " + token);
    }

    System.out.println("\nTesting retrieval:");
    for (int i = 0; i <= 70; i += 5) {
      OffsetTokenCache.TokenEntry entry = cache.getNearestToken(query, i);
      if (entry != null) {
        System.out.println("Query offset " + i + " -> Found token at offset " + entry.getOffset());
      } else {
        System.out.println("Query offset " + i + " -> NULL");
      }
    }

    System.out.println("\nCache stats: " + cache.getStats());
  }
}
