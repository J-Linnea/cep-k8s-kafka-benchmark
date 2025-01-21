package de.cau.se;

public class Event {

    private String caseid;
    private String activity;
    private String timestamp;
    private String node;
    private String group;

    public Event() {
    }

    public Event(
            final String caseid,
            final String activity,
            final String node,
            final String timestamp,
            final String group
    ) {
        this.caseid = caseid;
        this.activity = activity;
        this.timestamp = timestamp;
        this.node = node;
        this.group = group;
    }

    public void setCaseid(String caseid) {
        this.caseid = caseid;
    }

    public void setActivity(String activity) {
        this.activity = activity;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getCaseId() {
        return caseid;
    }

    public String getNode() {
        return node;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getActivity() {
        return activity;
    }

    public String getGroup() {
        return group;
    }

    @Override
    public String toString() {
        return "Event{" +
                "caseid='" + caseid + '\'' +
                ", activity='" + activity + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", node='" + node + '\'' +
                ", group='" + group + '\'' +
                '}';
    }
}
