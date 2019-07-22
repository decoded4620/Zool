package com.decoded.zool;

import java.util.Arrays;


public class ZConst {
  /**
   * Convenience for path separation
   */
  public enum PathSeparator {
    ZK("/"), WIN("\\"), NIX("/");

    private final String sep;

    PathSeparator(String sep) {
      this.sep = sep;
    }

    public String sep() {
      return sep;
    }

    public String join(String... parts) {
      return String.join(sep(), Arrays.asList(parts));
    }
  }
}
