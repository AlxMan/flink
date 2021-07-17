package com.wxc.java.state;

public class UserAction {
    private Long userId;
    private String userAction;

    public UserAction(Long userId, String userAction) {
        this.userId = userId;
        this.userAction = userAction;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getUserAction() {
        return userAction;
    }

    public void setUserAction(String userAction) {
        this.userAction = userAction;
    }

    @Override
    public String toString() {
        return "UserAction{" +
                "userId=" + userId +
                ", userAction='" + userAction + '\'' +
                '}';
    }
}
