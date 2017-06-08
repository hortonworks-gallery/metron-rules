package com.hortonworks.metron.loader.csv;

import java.io.PrintStream;

public class Progress {
  private int count = 0;
  private String anim= "|/-\\";
  private PrintStream pw;
  public Progress(PrintStream pw) {
    this.pw = pw;
  }

  public Progress() {
    this(System.out);
  }

  public synchronized void reset() {
    count = 0;
  }

  public synchronized void update() {
    int currentCount = count++;
    pw.print("\rProcessed " + currentCount + " - " + anim.charAt(currentCount % anim.length()));
  }

}
