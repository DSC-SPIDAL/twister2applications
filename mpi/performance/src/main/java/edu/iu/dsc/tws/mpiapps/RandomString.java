package edu.iu.dsc.tws.mpiapps;

import java.security.SecureRandom;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;

public class RandomString {
  /**
   * Generate a random string.
   */
  public String nextString() {
    for (int idx = 0; idx < buf.length; ++idx)
      buf[idx] = symbols[random.nextInt(symbols.length)];
    return new String(buf);
  }

  public String nextRandomSizeString() {
    int next = (int) (random.nextDouble() * maxLength);
    char[] buf = new char[next];
    for (int idx = 0; idx < buf.length; ++idx) {
      buf[idx] = symbols[random.nextInt(symbols.length)];
    }
    return new String(buf);
  }

  public static final String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  public static final String lower = upper.toLowerCase(Locale.ROOT);

  public static final String digits = "0123456789";

  public static final String alphanum = upper + lower + digits;

  private final Random random;

  private final char[] symbols;

  private final char[] buf;

  private int maxLength;

  public RandomString(int length, Random random, String symbols) {
    if (length < 1) throw new IllegalArgumentException();
    if (symbols.length() < 2) throw new IllegalArgumentException();
    this.random = Objects.requireNonNull(random);
    this.symbols = symbols.toCharArray();
    this.buf = new char[length];
    this.maxLength = length;
  }

  /**
   * Create an alphanumeric string generator.
   */
  public RandomString(int length, Random random) {
    this(length, random, alphanum);
  }

  /**
   * Create an alphanumeric strings from a secure generator.
   */
  public RandomString(int length) {
    this(length, new SecureRandom());
  }

  public static void main(String[] args) {
    RandomString r = new RandomString(100, new Random(), alphanum);
    for (int i = 0; i < 1000; i++) {
      System.out.println(r.nextRandomSizeString());
    }
  }
}
