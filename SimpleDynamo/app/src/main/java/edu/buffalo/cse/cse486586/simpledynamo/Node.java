package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.Objects;

public class Node implements Serializable {

    String myPort;
    String successorPort;
    String predecessorPort;

    public Node(String myPort, String successorPort, String predecessorPort) {
        this.myPort = myPort;
        this.successorPort = successorPort;
        this.predecessorPort = predecessorPort;
    }

    public String getMyPort() {
        return myPort;
    }

    public void setMyPort(String myPort) {
        this.myPort = myPort;
    }

    public String getSuccessorPort() {
        return successorPort;
    }

    public void setSuccessorPort(String successorPort) {
        this.successorPort = successorPort;
    }

    public String getPredecessorPort() {
        return predecessorPort;
    }

    public void setPredecessorPort(String predecessorPort) {
        this.predecessorPort = predecessorPort;
    }

    @Override
    public String toString() {
        return "Node{" +
                "myPort='" + myPort + '\'' +
                ", successorPort='" + successorPort + '\'' +
                ", predecessorPort='" + predecessorPort + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return Objects.equals(myPort, node.myPort) &&
                Objects.equals(successorPort, node.successorPort) &&
                Objects.equals(predecessorPort, node.predecessorPort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(myPort, successorPort, predecessorPort);
    }
}
