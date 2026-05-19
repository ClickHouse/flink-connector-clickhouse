package com.example;

import java.io.Serializable;

/**
 * Demo POJO. Build A populates only {@code a} and {@code b}; Build B adds {@code c}.
 *
 * <p>Note: this POJO is only the user's input shape — the connector no longer
 * serializes POJOs into Flink state. The mapper turns the POJO into a Map; the
 * Map is what's checkpointed.
 */
public class Event implements Serializable {

    private int a;
    private String b;
    private String c; // populated only by Build B

    public Event() {}

    public Event(int a, String b) {
        this.a = a;
        this.b = b;
    }

    public Event(int a, String b, String c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    public int getA() { return a; }
    public String getB() { return b; }
    public String getC() { return c; }

    public void setA(int a) { this.a = a; }
    public void setB(String b) { this.b = b; }
    public void setC(String c) { this.c = c; }
}
