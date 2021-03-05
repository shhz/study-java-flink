package com.study.beans;

/**
 * @author shsq
 * @description: CollectionSourceBean 数组数据按测试实体类
 * @date 2021/3/5 10:55
 */
public class CollectionSourceBean {
    // id，时间戳，长浮点
    private String id;
    private Long timestamp;
    private Double temperature;

    @Override
    public String toString() {
        return "CollectionSourceBean{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }

    public CollectionSourceBean(String id, Long timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public CollectionSourceBean() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }
}
