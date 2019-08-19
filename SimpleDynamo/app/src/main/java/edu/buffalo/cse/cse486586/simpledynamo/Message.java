package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

public class Message extends Node implements Serializable {

    String message;

    public Message(String myPort, String successorPort, String predecessorPort, String message) {
        super(myPort, successorPort, predecessorPort);
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "Message{" +
                "message='" + message + '\'' +
                ", myPort='" + myPort + '\'' +
                ", successorPort='" + successorPort + '\'' +
                ", predecessorPort='" + predecessorPort + '\'' +
                '}';
    }
}
