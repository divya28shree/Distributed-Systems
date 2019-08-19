package edu.buffalo.cse.cse486586.groupmessenger2;

public class QueueElement implements Comparable<QueueElement>{

    String msg;
    double proposedPriority;
   // String sourcePort;
    //String processor;
    int state;//1:not deliverable, 2:deliverable

    public String getSourcePort() {
        return sourcePort;
    }

    public void setSourcePort(String sourcePort) {
        this.sourcePort = sourcePort;
    }

    String sourcePort;



    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public double getProposedPriority() {
        return proposedPriority;
    }

    public void setProposedPriority(double proposedPriority) {
        this.proposedPriority = proposedPriority;
    }



    public QueueElement(String msg, double proposedPriority, int state, String sourcePort) {
        this.msg = msg;
        this.proposedPriority = proposedPriority;
        this.sourcePort = sourcePort;
        //this.processor = processor;
        this.state  = state;
    }

    @Override
    public int compareTo(QueueElement queueElement) {
        if (this.getProposedPriority() > queueElement.getProposedPriority()) {
            return 1;
        } else if (this.getProposedPriority() < queueElement.getProposedPriority()) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public boolean equals(Object o){
        // If the object is compared with itself then return true
        if (o == this) {
            return true;
        }

        /* Check if o is an instance of QueueElement or not
          "null instanceof [type]" also returns false */
        if (!(o instanceof QueueElement)) {
            return false;
        }

        QueueElement p = (QueueElement)o;
        return p.msg==this.msg;
        //IntegerArray x = (IntegerArray)o;
        //return this.a[0] == x.a[0] && this.a[1] == x.a[1];
    }
}
