package alien.priority;


public class User {
    private int id;
    private double defaultPriority;
    private int maxParallelJobs;
    private int runningJobs;

    public User(int id, double defaultPriority, int maxParallelJobs, int runningJobs) {
        this.id = id;
        this.defaultPriority = defaultPriority;
        this.maxParallelJobs = maxParallelJobs;
        this.runningJobs = runningJobs;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public double getDefaultPriority() {
        return defaultPriority;
    }

    public void setDefaultPriority(double defaultPriority) {
        this.defaultPriority = defaultPriority;
    }

    public int getMaxParallelJobs() {
        return maxParallelJobs;
    }

    public void setMaxParallelJobs(int maxParallelJobs) {
        this.maxParallelJobs = maxParallelJobs;
    }

    public int getRunningJobs() {
        return runningJobs;
    }

    public void setRunningJobs(int runningJobs) {
        this.runningJobs = runningJobs;
    }
}
