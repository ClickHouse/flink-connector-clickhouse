package com.example;

import java.io.Serializable;

/**
 * V1 of the demo POJO: two fields. The V2 release will add a {@code ts} field.
 *
 * <p>Schema evolution in Flink works on class identity — the FQ name
 * {@code com.example.EvolvingPOJO} stays the same across releases; only the
 * field set changes. {@code PojoSerializerSnapshot} compares the snapshot's
 * field list to the current field list and applies field-add migration when
 * the names match.
 */
public class EvolvingPOJO implements Serializable {
    private static final long serialVersionUID = 1L;

    public int id;
    public String name;

    public EvolvingPOJO() {}

    public EvolvingPOJO(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() { return id; }
    public void setId(int id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
}
