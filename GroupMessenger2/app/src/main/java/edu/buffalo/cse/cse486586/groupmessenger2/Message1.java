package edu.buffalo.cse.cse486586.groupmessenger2;

import java.io.Serializable;

public class Message1 implements Serializable{

    String msg;
    int msgType;//1: ask for proposal, 2:final seq number, 3:sending proposal
    String senderPort;
    double proposal;
    int count;//Number of message generated in a PEs
    int state;//1:not deliverable, 2:deliverable


    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public int getMsgType() {
        return msgType;
    }

    public void setMsgType(int msgType) {
        this.msgType = msgType;
    }

    public String getSenderPort() {
        return senderPort;
    }

    public void setSenderPort(String senderPort) {
        this.senderPort = senderPort;
    }

    public double getProposal() {
        return proposal;
    }

    public void setProposal(double proposal) {
        this.proposal = proposal;
    }

    public Message1(String msg, int msgType, String senderPort, double proposal) {
        this.msg = msg;
        this.msgType = msgType;
        this.senderPort = senderPort;
        this.proposal = proposal;
        //this.count = count;
    }
}
