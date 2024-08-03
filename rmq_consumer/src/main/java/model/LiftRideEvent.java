package model;


public class LiftRideEvent {

    private int skierID;
    private int resortID;
    private String seasonID;
    private String dayID;
    private LiftRide liftRide;

    private String timestamp;

    // Constructor

    public LiftRideEvent() {
    }

    public LiftRideEvent(int resortID, String seasonID, String dayID, int skierID, LiftRide liftRide) {
        this.resortID = resortID;
        this.seasonID = seasonID;
        this.dayID = dayID;
        this.skierID = skierID;
        this.liftRide = liftRide;
    }
    public String getTimestamp() {
        return timestamp;
    }
    public void setSkierID(int skierID) {
        this.skierID = skierID;
    }

    public void setResortID(int resortID) {
        this.resortID = resortID;
    }

    public void setSeasonID(String seasonID) {
        this.seasonID = seasonID;
    }

    public void setDayID(String dayID) {
        this.dayID = dayID;
    }

    public void setLiftRide(LiftRide liftRide) {
        this.liftRide = liftRide;
    }

    public int getSkierID() {
        return skierID;
    }

    public int getResortID() {
        return resortID;
    }

    public String getSeasonID() {
        return seasonID;
    }

    public String getDayID() {
        return dayID;
    }

    public LiftRide getLiftRide() {
        return liftRide;
    }


    @Override
    public String toString() {
        return "LiftRideEvent{" +
                "skierID=" + skierID +
                ", resortID=" + resortID +
                ", seasonID='" + seasonID + '\'' +
                ", dayID='" + dayID + '\'' +
                ", liftRide=" + liftRide +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
