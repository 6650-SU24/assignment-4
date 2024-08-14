package org.example;
import io.swagger.client.model.LiftRide;

import java.util.Random;
public class LiftRideEvent {
    private static final Random random = new Random();

    private int skierID;
    private int resortID;
    private int seasonID;
    private int dayID;
    private LiftRide liftRide;

    // Constructor
    public LiftRideEvent(int resortID, int seasonID, int dayID, int skierID, LiftRide liftRide) {
        this.resortID = resortID;
        this.seasonID = seasonID;
        this.dayID = dayID;
        this.skierID = skierID;
        this.liftRide = liftRide;
    }

    public int getSkierID() {
        return skierID;
    }

    public int getResortID() {
        return resortID;
    }

    public int getSeasonID() {
        return seasonID;
    }

    public int getDayID() {
        return dayID;
    }

    public LiftRide getLiftRide() {
        return liftRide;
    }

    public static LiftRideEvent generate() {
//        int resortID = random.nextInt(10) + 1;
        int resortID = 8;
//        int seasonID = random.nextInt(10) + 2015;
        int seasonID = 2024;
//        int dayID = random.nextInt(366) + 1;
        int dayID = 1;
        int skierID = random.nextInt(100000) + 1;
        LiftRide liftRide = new LiftRide();
        liftRide.setLiftID(random.nextInt(40) + 1);
        liftRide.setTime(random.nextInt(360) + 1); // 9:00 - 15:00
        return new LiftRideEvent( resortID, seasonID, dayID, skierID, liftRide);
    }

    @Override
    public String toString() {
        return "LiftRideEvent{" +
                "skierID=" + skierID +
                ", resortID=" + resortID +
                ", seasonID='" + seasonID + '\'' +
                ", dayID='" + dayID + '\'' +
                ", liftRide=" + liftRide +
                '}';
    }

    public String toCsv() {
        return skierID + "," + resortID + "," + seasonID + "," + dayID+ "," + liftRide.getLiftID()+ "," + liftRide.getTime();
    }
}
