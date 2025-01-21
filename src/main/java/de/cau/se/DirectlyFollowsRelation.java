package de.cau.se;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

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
    public boolean equals(Object obj) {
        if (!(obj instanceof DirectlyFollowsRelation)) {
            return false;
        }
        DirectlyFollowsRelation other = (DirectlyFollowsRelation) obj;
        EqualsBuilder builder = new EqualsBuilder();
        return builder.append(predecessor, other.predecessor).append(successor, other.successor).build();
    }

    @Override
    public int hashCode() {
        final HashCodeBuilder hashCodeBuilder = new HashCodeBuilder();
        hashCodeBuilder.append(predecessor);
        hashCodeBuilder.append(successor);
        return hashCodeBuilder.hashCode();
    }

    @Override
    public String toString() {
        return "de.cau.se.DirectlyFollows{" +
                "predecessor='" + predecessor + '\'' +
                ", successor='" + successor + '\'' +
                '}';
    }
}
