package com.cchux.report.bean;

/**
 * 定义消息的bean对象
 */
public class Message {
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    //消息的内容
    private String message;

    //消息的事件时间
    private String timestamp;

    //点击次数
    private int count;


}
