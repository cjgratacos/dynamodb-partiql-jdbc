package org.cjgratacos.jdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Utility class for generating test data for DynamoDB tables.
 *
 * <p>This class provides methods to generate consistent, predictable test data for testing schema
 * discovery and performance optimization features.
 */
public class TestDataGenerator {

  // Fixed seed for reproducible tests - used for future randomization features
  @SuppressWarnings("unused")
  private static final Random RANDOM = new Random(42);

  private static final String[] SAMPLE_NAMES = {
    "John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"
  };

  private static final String[] SAMPLE_CITIES = {
    "New York",
    "Los Angeles",
    "Chicago",
    "Houston",
    "Phoenix",
    "Philadelphia",
    "San Antonio",
    "San Diego",
    "Dallas",
    "San Jose"
  };

  private static final String[] SAMPLE_DEPARTMENTS = {
    "Engineering", "Marketing", "Sales", "HR", "Finance", "Operations", "Support", "Legal"
  };

  /**
   * Generates a test item for a table with varied attribute types.
   *
   * @param tableName the name of the table (affects data generation)
   * @param index the index of the item (used for consistent generation)
   * @return a map representing the DynamoDB item
   */
  public static Map<String, AttributeValue> generateTestItem(String tableName, int index) {
    final var item = new HashMap<String, AttributeValue>();

    // Always include an ID field
    item.put("id", AttributeValue.builder().s(String.format("%s_%d", tableName, index)).build());

    // Generate different types of data based on table name and index
    switch (tableName.toLowerCase()) {
      case "users":
      case "user":
        generateUserData(item, index);
        break;
      case "orders":
      case "order":
        generateOrderData(item, index);
        break;
      case "products":
      case "product":
        generateProductData(item, index);
        break;
      default:
        generateGenericData(item, index);
        break;
    }

    return item;
  }

  /**
   * Generates multiple test items for a table.
   *
   * @param tableName the name of the table
   * @param count the number of items to generate
   * @return a list of generated items
   */
  public static List<Map<String, AttributeValue>> generateTestItems(String tableName, int count) {
    final var items = new ArrayList<Map<String, AttributeValue>>();
    for (int i = 0; i < count; i++) {
      items.add(generateTestItem(tableName, i));
    }
    return items;
  }

  /**
   * Generates an item with conflicting types to test type resolution.
   *
   * @param attributeName the name of the attribute
   * @param index the index to determine the type
   * @return an AttributeValue with varying types
   */
  public static AttributeValue generateConflictingTypeValue(String attributeName, int index) {
    // Generate different types based on index to test type conflict resolution
    switch (index % 4) {
      case 0:
        return AttributeValue.builder().s("string_value_" + index).build();
      case 1:
        return AttributeValue.builder().n(String.valueOf(index)).build();
      case 2:
        return AttributeValue.builder().bool(index % 2 == 0).build();
      case 3:
        return AttributeValue.builder().nul(true).build();
      default:
        return AttributeValue.builder().s("default_" + index).build();
    }
  }

  /**
   * Generates an item with nested structure for complex schema testing.
   *
   * @param index the index of the item
   * @return a map with nested attributes
   */
  public static Map<String, AttributeValue> generateNestedItem(int index) {
    final var item = new HashMap<String, AttributeValue>();

    item.put("id", AttributeValue.builder().s("nested_" + index).build());

    // Nested map
    final var address = new HashMap<String, AttributeValue>();
    address.put("street", AttributeValue.builder().s(index + " Main St").build());
    address.put(
        "city", AttributeValue.builder().s(SAMPLE_CITIES[index % SAMPLE_CITIES.length]).build());
    address.put(
        "zipcode", AttributeValue.builder().s(String.format("%05d", 10000 + index)).build());

    item.put("address", AttributeValue.builder().m(address).build());

    // List of strings
    final var tags = new ArrayList<AttributeValue>();
    tags.add(AttributeValue.builder().s("tag_" + index).build());
    tags.add(AttributeValue.builder().s("category_" + (index % 3)).build());

    item.put("tags", AttributeValue.builder().l(tags).build());

    return item;
  }

  private static void generateUserData(Map<String, AttributeValue> item, int index) {
    item.put("name", AttributeValue.builder().s(SAMPLE_NAMES[index % SAMPLE_NAMES.length]).build());
    item.put(
        "email", AttributeValue.builder().s(String.format("user%d@example.com", index)).build());
    item.put("age", AttributeValue.builder().n(String.valueOf(20 + (index % 50))).build());
    item.put("active", AttributeValue.builder().bool(index % 3 != 0).build());
    item.put(
        "city", AttributeValue.builder().s(SAMPLE_CITIES[index % SAMPLE_CITIES.length]).build());

    // Sometimes add optional fields
    if (index % 4 == 0) {
      item.put(
          "department",
          AttributeValue.builder()
              .s(SAMPLE_DEPARTMENTS[index % SAMPLE_DEPARTMENTS.length])
              .build());
    }

    if (index % 5 == 0) {
      item.put(
          "salary", AttributeValue.builder().n(String.valueOf(50000 + (index * 1000))).build());
    }
  }

  private static void generateOrderData(Map<String, AttributeValue> item, int index) {
    item.put("customerId", AttributeValue.builder().s("customer_" + (index % 100)).build());
    item.put(
        "total", AttributeValue.builder().n(String.format("%.2f", 10.0 + (index * 5.5))).build());
    item.put("status", AttributeValue.builder().s(getOrderStatus(index)).build());
    item.put(
        "orderDate",
        AttributeValue.builder().s("2024-01-" + String.format("%02d", 1 + (index % 30))).build());

    // Sometimes add optional fields
    if (index % 3 == 0) {
      item.put("discount", AttributeValue.builder().n(String.valueOf(index % 20)).build());
    }

    if (index % 7 == 0) {
      item.put("notes", AttributeValue.builder().s("Special order " + index).build());
    }
  }

  private static void generateProductData(Map<String, AttributeValue> item, int index) {
    item.put("name", AttributeValue.builder().s("Product " + index).build());
    item.put(
        "price", AttributeValue.builder().n(String.format("%.2f", 5.0 + (index * 2.3))).build());
    item.put("category", AttributeValue.builder().s(getProductCategory(index)).build());
    item.put("inStock", AttributeValue.builder().bool(index % 4 != 0).build());
    item.put("quantity", AttributeValue.builder().n(String.valueOf(index % 100)).build());

    // Sometimes add optional fields
    if (index % 2 == 0) {
      item.put(
          "description", AttributeValue.builder().s("Description for product " + index).build());
    }

    if (index % 6 == 0) {
      item.put(
          "weight", AttributeValue.builder().n(String.format("%.1f", 0.5 + (index * 0.2))).build());
    }
  }

  private static void generateGenericData(Map<String, AttributeValue> item, int index) {
    item.put("name", AttributeValue.builder().s("Item " + index).build());
    item.put("value", AttributeValue.builder().n(String.valueOf(index)).build());
    item.put("flag", AttributeValue.builder().bool(index % 2 == 0).build());

    // Add some variety with optional fields
    if (index % 3 == 0) {
      item.put("optional_string", AttributeValue.builder().s("optional_" + index).build());
    }

    if (index % 5 == 0) {
      item.put("optional_number", AttributeValue.builder().n(String.valueOf(index * 10)).build());
    }

    // Sometimes include null values to test nullable detection
    if (index % 10 == 0) {
      item.put("nullable_field", AttributeValue.builder().nul(true).build());
    }
  }

  private static String getOrderStatus(int index) {
    final String[] statuses = {"pending", "processing", "shipped", "delivered", "cancelled"};
    return statuses[index % statuses.length];
  }

  private static String getProductCategory(int index) {
    final String[] categories = {"electronics", "clothing", "books", "home", "sports", "toys"};
    return categories[index % categories.length];
  }
}
