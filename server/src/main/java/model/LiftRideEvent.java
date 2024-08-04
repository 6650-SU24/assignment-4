package model;


import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

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

    public void setTimestamp() {
        int startHour = 9;
        int startMinute = 0;

        int hours = this.liftRide.getTime() / 60;
        int minutes = this.liftRide.getTime() % 60;

        LocalDate date = LocalDate.of(Integer.parseInt(this.seasonID), 1, 1).plusDays(Integer.parseInt(this.dayID) - 1);
        LocalTime time = LocalTime.of(startHour + hours, startMinute + minutes, new Random().nextInt(60), new Random().nextInt(1000000000));
        LocalDateTime dateTime = LocalDateTime.of(date, time);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nnnnnnnnn");
        this.timestamp = dateTime.format(formatter);

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

    public String getTimestamp() {
        return timestamp;
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
