package org.cjgratacos.jdbc;

/** Data class to hold extracted LIMIT and OFFSET values from SQL queries. */
public class LimitOffsetInfo {
  private final Integer limit;
  private final Integer offset;

  /**
   * Constructs a new LimitOffsetInfo instance.
   *
   * @param limit the LIMIT value, or null if not specified
   * @param offset the OFFSET value, or null if not specified
   */
  public LimitOffsetInfo(Integer limit, Integer offset) {
    this.limit = limit;
    this.offset = offset;
  }

  /**
   * Gets the LIMIT value.
   *
   * @return the LIMIT value, or null if not specified
   */
  public Integer getLimit() {
    return limit;
  }

  /**
   * Gets the OFFSET value.
   *
   * @return the OFFSET value, or null if not specified
   */
  public Integer getOffset() {
    return offset;
  }

  /**
   * Checks if a LIMIT value was specified.
   *
   * @return true if LIMIT was specified, false otherwise
   */
  public boolean hasLimit() {
    return limit != null;
  }

  /**
   * Checks if an OFFSET value was specified.
   *
   * @return true if OFFSET was specified, false otherwise
   */
  public boolean hasOffset() {
    return offset != null;
  }

  @Override
  public String toString() {
    return "LimitOffsetInfo{" + "limit=" + limit + ", offset=" + offset + '}';
  }
}
