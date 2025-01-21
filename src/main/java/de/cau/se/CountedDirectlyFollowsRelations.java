package de.cau.se;

public class CountedDirectlyFollowsRelations {

    private DirectlyFollowsRelation directlyFollowsRelation;
    private int count;

    public CountedDirectlyFollowsRelations() {
    }

    public CountedDirectlyFollowsRelations(DirectlyFollowsRelation directlyFollowsRelation, int count) {
        this.directlyFollowsRelation = directlyFollowsRelation;
        this.count = count;
    }

    public DirectlyFollowsRelation getDirectlyFollowsRelation() {
        return directlyFollowsRelation;
    }

    public void setDirectlyFollowsRelation(DirectlyFollowsRelation directlyFollowsRelation) {
        this.directlyFollowsRelation = directlyFollowsRelation;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "CountedDirectlyFollowsRelations{" +
                "directlyFollowsRelation=" + directlyFollowsRelation +
                ", count=" + count +
                '}';
    }
}
