package edu.buffalo.cse.cse486586.simpledht;

public class KeyPriorityQueue  {

    String keyHash;
    String value;

    public KeyPriorityQueue(String keyHash, String value) {
        this.keyHash = keyHash;
        this.value = value;
    }

    public String getKeyHash() {
        return keyHash;
    }

    public void setKeyHash(String keyHash) {
        this.keyHash = keyHash;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
