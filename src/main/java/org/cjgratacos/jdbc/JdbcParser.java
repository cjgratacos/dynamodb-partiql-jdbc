package org.cjgratacos.jdbc;

import java.util.Properties;

/**
 * Utility class for parsing JDBC URL properties in DynamoDB PartiQL driver.
 *
 * <p>This class provides static methods to extract key-value properties from DynamoDB JDBC URL
 * strings. It handles the specific URL format used by this driver and converts them into a standard
 * Java Properties object.
 *
 * <h2>Supported URL Format:</h2>
 *
 * <pre>
 * jdbc:dynamodb:partiql:region=&lt;region&gt;;credentialsType=&lt;type&gt;;[additional_properties];
 * </pre>
 *
 * <h2>Parsing Logic:</h2>
 *
 * <p>The parser identifies properties by finding the last colon (':') before the first equals sign
 * ('='). Everything after this colon is treated as property definitions in the format: {@code
 * key1=value1;key2=value2;...}
 *
 * <h2>Property Examples:</h2>
 *
 * <ul>
 *   <li><strong>Basic</strong>: {@code jdbc:dynamodb:partiql:region=us-east-1;}
 *   <li><strong>With credentials</strong>: {@code
 *       jdbc:dynamodb:partiql:region=us-east-1;credentialsType=STATIC;accessKey=AKIAI...;secretKey=wJal...;}
 *   <li><strong>With endpoint</strong>: {@code
 *       jdbc:dynamodb:partiql:region=us-east-1;endpoint=http://localhost:8000;}
 *   <li><strong>With schema discovery</strong>: {@code
 *       jdbc:dynamodb:partiql:region=us-east-1;schemaDiscovery=auto;sampleSize=1000;sampleStrategy=random;}
 *   <li><strong>With schema cache</strong>: {@code
 *       jdbc:dynamodb:partiql:region=us-east-1;schemaCache=true;schemaCacheTTL=3600;}
 * </ul>
 *
 * <h2>Error Handling:</h2>
 *
 * <p>If no properties are found in the URL, an empty Properties object is returned. Invalid
 * property formats are silently ignored, allowing for graceful degradation.
 *
 * <h2>Example Usage:</h2>
 *
 * <pre>{@code
 * String url = "jdbc:dynamodb:partiql:region=us-east-1;credentialsType=DEFAULT;endpoint=http://localhost:8000;";
 * Properties props = JdbcParser.extractProperties(url);
 *
 * System.out.println(props.getProperty("region"));         // "us-east-1"
 * System.out.println(props.getProperty("credentialsType"));// "DEFAULT"
 * System.out.println(props.getProperty("endpoint"));       // "http://localhost:8000"
 * }</pre>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 * @see DynamoDbDriver
 * @see DynamoDbConnection
 */
public class JdbcParser {

  /** Private constructor to prevent instantiation of this utility class. */
  private JdbcParser() {
    // Utility class - no instantiation
  }

  /**
   * Extracts connection properties from a DynamoDB JDBC URL.
   *
   * <p>This method parses the URL to find property definitions after the last colon before the
   * first equals sign. Properties are expected in the format: {@code key1=value1;key2=value2;...}
   *
   * <p>The parsing process:
   *
   * <ol>
   *   <li>Finds the first equals sign in the URL
   *   <li>Locates the last colon before that equals sign
   *   <li>Extracts everything after that colon as the properties section
   *   <li>Splits by semicolons and parses each key=value pair
   * </ol>
   *
   * @param url the JDBC URL to parse (null and empty strings are handled gracefully)
   * @return a Properties object containing the extracted key-value pairs, or an empty Properties
   *     object if no properties are found or URL is null/empty
   * @see Properties
   * @see DynamoDbDriver#connect(String, Properties)
   */
  public static Properties extractProperties(final String url) {
    final var props = new Properties();

    // Handle null or empty URL
    if (url == null || url.isEmpty()) {
      return props;
    }

    // Find where properties start (after the last ':' before the first '=')
    final var firstEquals = url.indexOf('=');
    if (firstEquals == -1) {
      return props; // No properties found
    }

    // Find the last ':' before the first '='
    final var propStart = url.lastIndexOf(':', firstEquals);
    if (propStart == -1) {
      return props; // No prefix found
    }

    // Extract properties section (everything after the last prefix ':')
    var propString = url.substring(propStart + 1);

    // Remove trailing semicolon (which should always be there)
    if (propString.endsWith(";")) {
      propString = propString.substring(0, propString.length() - 1);
    }

    // Split by ';' and parse each key=value pair
    final var pairs = propString.split(";");
    for (final var pair : pairs) {
      if (pair.isEmpty()) continue;
      final var kv = pair.split("=", 2);
      if (kv.length == 2) {
        props.setProperty(kv[0].trim(), kv[1].trim());
      }
    }

    return props;
  }
}
