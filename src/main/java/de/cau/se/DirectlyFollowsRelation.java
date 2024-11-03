package de.cau.se;

public class DirectlyFollowsRelation {
    private String predecessor;
    private String successor;

    public DirectlyFollowsRelation() {
    }

    public DirectlyFollowsRelation(String predecessor, String successor) {
        this.predecessor = predecessor;
        this.successor = successor;
    }

    public String getPredecessor() {
        return predecessor;
    }

    public String getSuccessor() {
        return successor;
    }

    public void setPredecessor(String predecessor) {
        this.predecessor = predecessor;
    }

    public void setSuccessor(String successor) {
        this.successor = successor;
    }

    @Override
    public String toString() {
        return "de.cau.se.DirectlyFollows{" +
                "predecessor='" + predecessor + '\'' +
                ", successor='" + successor + '\'' +
                '}';
    }
}
