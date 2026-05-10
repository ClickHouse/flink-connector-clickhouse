package com.example;

import java.io.Serializable;

/**
 * V2 of the demo POJO: same FQ name as V1, with one new field {@code ts}.
 *
 * <p>Schema evolution in Flink works on class identity — the FQ name
 * {@code com.example.EvolvingPOJO} is preserved across releases; only the
 * field set has changed. {@code PojoSerializerSnapshot} compares the snapshot
 * (written by V1) to this current shape, sees that {@code ts} is new, and
 * builds a reconfigured serializer that reads V1 bytes and constructs V2
 * instances with {@code ts=0L} (Java default for {@code long}).
 */
public class EvolvingPOJO implements Serializable {
    private static final long serialVersionUID = 1L;

    public int id;
    public String name;
    public long ts;

    public EvolvingPOJO() {}

    public EvolvingPOJO(int id, String name, long ts) {
        this.id = id;
        this.name = name;
        this.ts = ts;
    }

    public int getId() { return id; }
    public void setId(int id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public long getTs() { return ts; }
    public void setTs(long ts) { this.ts = ts; }
}
