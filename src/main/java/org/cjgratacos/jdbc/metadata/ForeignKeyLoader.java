package org.cjgratacos.jdbc.metadata;

import java.util.List;

/**
 * Interface for loading foreign key metadata from different sources. Implementations can load
 * foreign keys from files, databases, or other sources.
 */
public interface ForeignKeyLoader {

  /**
   * Loads foreign key metadata from the configured source.
   *
   * @param source the source identifier (e.g., file path, table name)
   * @return list of foreign key metadata
   * @throws ForeignKeyLoadException if loading fails
   */
  List<ForeignKeyMetadata> load(String source) throws ForeignKeyLoadException;

  /**
   * Validates if the source is accessible and properly formatted.
   *
   * @param source the source identifier to validate
   * @return true if the source is valid, false otherwise
   */
  boolean isValidSource(String source);
}
