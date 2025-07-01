package org.cjgratacos.jdbc.metadata;

import java.sql.SQLException;
import java.util.List;

/**
 * Exception thrown when foreign key validation fails. Extends SQLException for JDBC compatibility.
 */
public class ForeignKeyValidationException extends SQLException {

  /** The validation errors that caused this exception. */
  private final List<String> validationErrors;

  /**
   * Creates a new ForeignKeyValidationException with a message.
   *
   * @param message the error message
   */
  public ForeignKeyValidationException(String message) {
    super(message);
    this.validationErrors = null;
  }

  /**
   * Creates a new ForeignKeyValidationException with a message and cause.
   *
   * @param message the error message
   * @param cause the underlying cause
   */
  public ForeignKeyValidationException(String message, Throwable cause) {
    super(message, cause);
    this.validationErrors = null;
  }

  /**
   * Creates a new ForeignKeyValidationException with a message and list of validation errors.
   *
   * @param message the error message
   * @param validationErrors list of specific validation errors
   */
  public ForeignKeyValidationException(String message, List<String> validationErrors) {
    super(message);
    this.validationErrors = validationErrors;
  }

  /**
   * Gets the list of validation errors if available.
   *
   * @return list of validation errors or null
   */
  public List<String> getValidationErrors() {
    return validationErrors;
  }

  /**
   * Creates a formatted error message from multiple validation errors.
   *
   * @param errors the list of validation errors
   * @return formatted error message
   */
  public static String formatErrors(List<String> errors) {
    if (errors == null || errors.isEmpty()) {
      return "Foreign key validation failed";
    }

    StringBuilder sb =
        new StringBuilder("Foreign key validation failed with ")
            .append(errors.size())
            .append(" error(s):\n");

    for (int i = 0; i < errors.size(); i++) {
      sb.append("  ").append(i + 1).append(". ").append(errors.get(i));
      if (i < errors.size() - 1) {
        sb.append("\n");
      }
    }

    return sb.toString();
  }
}
