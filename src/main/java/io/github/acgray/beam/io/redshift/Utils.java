package io.github.acgray.beam.io.redshift;

public interface Utils {
  @SafeVarargs
  static <T> T coalesce(T ...args) {
    for (T val : args) {
      if (val != null) {
        return val;
      }
    }
    return null;
  }
}
