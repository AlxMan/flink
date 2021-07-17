package com.wxc.java.state;

public class MyPattern {
    private String firstAction;
    private String secondAction;

    public MyPattern() {

    }

    public MyPattern(String firstAction, String secondAction) {
        this.firstAction = firstAction;
        this.secondAction = secondAction;
    }

    public String getFirstAction() {
        return firstAction;
    }

    public void setFirstAction(String firstAction) {
        this.firstAction = firstAction;
    }

    public String getSecondAction() {
        return secondAction;
    }

    public void setSecondAction(String secondAction) {
        this.secondAction = secondAction;
    }

    @Override
    public String toString() {
        return "MyPattern{" +
                "firstAction='" + firstAction + '\'' +
                ", secondAction='" + secondAction + '\'' +
                '}';
    }
}
