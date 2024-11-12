package alien.priority;

public class RunningJobs {
    private int counterTotalRunningJobs;
    private int userId;
    private int userCurrentlyRunningJobs;
    private double computedPriorityAtJobStart;
    private float currentUserCost;


    public RunningJobs(int counterTotalRunningJobs, int userId, int userCurrentlyRunningJobs, double computedPriorityAtJobStart, float currentUserCost) {
        this.counterTotalRunningJobs = counterTotalRunningJobs;
        this.userId = userId;
        this.userCurrentlyRunningJobs = userCurrentlyRunningJobs;
        this.computedPriorityAtJobStart = computedPriorityAtJobStart;
        this.currentUserCost = currentUserCost;
    }

    public int getCounterTotalRunningJobs() {
        return counterTotalRunningJobs;
    }

    public void setCounterTotalRunningJobs(int counterTotalRunningJobs) {
        this.counterTotalRunningJobs = counterTotalRunningJobs;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getUserCurrentlyRunningJobs() {
        return userCurrentlyRunningJobs;
    }

    public void setUserCurrentlyRunningJobs(int userCurrentlyRunningJobs) {
        this.userCurrentlyRunningJobs = userCurrentlyRunningJobs;
    }

    public double getComputedPriorityAtJobStart() {
        return computedPriorityAtJobStart;
    }

    public void setComputedPriorityAtJobStart(double computedPriorityAtJobStart) {
        this.computedPriorityAtJobStart = computedPriorityAtJobStart;
    }

    public float getCurrentUserCost() {
        return currentUserCost;
    }
}
