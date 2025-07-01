package org.cjgratacos.jdbc.metadata;

/** Exception thrown when foreign key loading fails. */
public class ForeignKeyLoadException extends Exception {

  /**
   * Creates a new ForeignKeyLoadException with a message.
   *
   * @param message the detail message
   */
  public ForeignKeyLoadException(String message) {
    super(message);
  }

  /**
   * Creates a new ForeignKeyLoadException with a message and cause.
   *
   * @param message the detail message
   * @param cause the cause of the exception
   */
  public ForeignKeyLoadException(String message, Throwable cause) {
    super(message, cause);
  }
}
