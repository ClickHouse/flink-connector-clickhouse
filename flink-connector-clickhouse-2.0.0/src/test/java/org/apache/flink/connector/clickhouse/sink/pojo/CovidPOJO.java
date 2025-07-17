package org.apache.flink.connector.clickhouse.sink.pojo;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class CovidPOJO {
    private LocalDate date;
    private String location_key;
    private Integer new_confirmed;
    private Integer new_deceased;
    private Integer new_recovered;
    private Integer new_tested;
    private Integer cumulative_confirmed;
    private Integer cumulative_deceased;
    private Integer cumulative_recovered;
    private Integer cumulative_tested;
    static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public CovidPOJO(String payload) {
        String[] values = payload.split(",");
        this.date = LocalDate.parse(values[0].trim(), formatter);
        this.location_key = values[1].trim();
        this.new_confirmed = valueOf(values[2].trim());
        this.new_deceased = valueOf(values[3].trim());
        this.new_recovered = valueOf(values[4].trim());
        this.new_tested = valueOf(values[5].trim());
        this.cumulative_confirmed = valueOf(values[6].trim());
        this.cumulative_deceased = valueOf(values[7].trim());
        this.cumulative_recovered = 0;
        this.cumulative_tested = 0;
    }

    private Integer valueOf(String value) {
        if (value == null || value.trim().isEmpty()) {
            return 0;
        } else {
            return Integer.valueOf(value);
        }
    }

    public LocalDate getLocalDate() {
        return date;
    }
    public void setDate(LocalDate date) {
        this.date = date;
    }
    public String getLocation_key() {
        return location_key;
    }
    public void setLocation_key(String location_key) {
        this.location_key = location_key;
    }
    public Integer getNew_confirmed() {
        return new_confirmed;
    }
    public void setNew_confirmed(Integer new_confirmed) {
        this.new_confirmed = new_confirmed;
    }
    public Integer getNew_deceased() {
        return new_deceased;
    }
    public void setNew_deceased(Integer new_deceased) {
        this.new_deceased = new_deceased;
    }
    public Integer getNew_recovered() {
        return new_recovered;
    }
    public void setNew_recovered(Integer new_recovered) {
        this.new_recovered = new_recovered;
    }
    public Integer getNew_tested() {
        return new_tested;
    }
    public void setNew_tested(Integer new_tested) {
        this.new_tested = new_tested;
    }
    public Integer getCumulative_confirmed() {
        return cumulative_confirmed;
    }
    public void setCumulative_confirmed(Integer cumulative_confirmed) {
        this.cumulative_confirmed = cumulative_confirmed;
    }
    public Integer getCumulative_deceased() {
        return cumulative_deceased;
    }
    public void setCumulative_deceased(Integer cumulative_deceased) {
        this.cumulative_deceased = cumulative_deceased;
    }
    public Integer getCumulative_recovered() {
        return cumulative_recovered;
    }
    public void setCumulative_recovered(Integer cumulative_recovered) {
        this.cumulative_recovered = cumulative_recovered;
    }
    public Integer getCumulative_tested() {
        return cumulative_tested;
    }
    public void setCumulative_tested(Integer cumulative_tested) {
        this.cumulative_tested = cumulative_tested;
    }
}
