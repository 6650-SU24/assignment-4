package model;

public class LiftRide {
    private Integer time;
    private Integer liftID;

    public LiftRide() {
    }

    public LiftRide(Integer time, Integer liftID) {
        this.time = time;
        this.liftID = liftID;
    }

    public Integer getTime() {
        return time;
    }

    public Integer getLiftID() {
        return liftID;
    }

    public void setTime(Integer time) {
        this.time = time;
    }

    public void setLiftID(Integer liftID) {
        this.liftID = liftID;
    }

    @Override
    public String toString() {
        return "LiftRide{" +
                "time=" + time +
                ", liftID=" + liftID +
                '}';
    }
}
