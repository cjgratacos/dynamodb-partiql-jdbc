package org.cjgratacos.jdbc.metadata;

/** Exception thrown when foreign key loading fails. */
public class ForeignKeyLoadException extends Exception {

  public ForeignKeyLoadException(String message) {
    super(message);
  }

  public ForeignKeyLoadException(String message, Throwable cause) {
    super(message, cause);
  }
}
