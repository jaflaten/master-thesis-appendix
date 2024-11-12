package alien.priority;

import lazyj.DBFunctions;

/**
 * @author Jorn-Are Flaten
 * @since 2023-12-04
 */
class PriorityDto {
	public PriorityDto(final DBFunctions db) {
		this.userId = (db.geti("userId") != 0) ? db.geti("userId") : 0;
		this.priority = (db.getf("priority") > 1) ? db.getf("priority") : 1f;
		this.maxParallelJobs = (db.geti("maxparallelJobs") > 1) ? db.geti("maxparallelJobs") : 1;
		this.userload = (db.getf("userload") > 0f) ? db.getf("userload") : 0f;
		this.maxTotalRunningTime = (db.getl("maxTotalRunningTime") > 1) ? db.getl("maxTotalRunningTime") : 1L;
		this.computedPriority = 1;
		this.totalRunningTimeLast24h = (db.getl("totalRunningTimeLast24h") > 0) ? db.getl("totalRunningTimeLast24h") : 0L;
		this.running = (db.geti("running") > 0) ? db.geti("running") : 0;
		this.totalCpuCostLast24h = (db.getf("totalCpuCostLast24h") > 0f) ? db.getf("totalCpuCostLast24h") : 0f;
		this.maxTotalCpuCost = (db.getf("maxTotalCpuCost") > 1f) ? db.getf("maxTotalCpuCost") : 1f;
		this.highestPriority = (db.getf("highestPriority") > 1f) ? db.getf("highestPriority") : 1f;
	}

	public PriorityDto() {
	}

	/**
	 * User id
	 */
	private int userId;

	/**
	 * User baseline priority
	 */
	private float priority;

	/**
	 * Maximum number of cpu cores that can be utilized simultaneously by a user
	 */
	private int maxParallelJobs;
	/**
	 * Current user load = runningJobs / maxParallelJobs
	 */
	private float userload;

	/**
	 * Max total cpu time that can be used by a user
	 */
	private long maxTotalRunningTime;

	/**
	 * Number of running jobs
	 */
	private int running;

	/**
	 * computed priority determines which user gets priority to run a job.
	 */
	private float computedPriority;

	/**
	 * Total cpu cost of all jobs of this user in the last 24 hours
	 */
	private float totalCpuCostLast24h;

	/**
	 * Maximum total cpu cost that can be used by a user
	 */
	private float maxTotalCpuCost;

	/**
	 * Total running time of all jobs of this user in the last 24 hours
	 */
	private long totalRunningTimeLast24h;

	/**
	 * Highest priority value of all users
	 */
	private float highestPriority;

	public PriorityDto(int userId, float priority, int maxParallelJobs, float userload, long maxTotalRunningTime, int running, float computedPriority, float maxTotalCpuCost,
			long totalRunningTimeLast24h, float totalCpuCostLast24h) {
		this.userId = userId;
		this.priority = priority;
		this.maxParallelJobs = maxParallelJobs;
		this.userload = userload;
		this.running = running;
		this.computedPriority = computedPriority;
		this.totalCpuCostLast24h = totalCpuCostLast24h;
		this.maxTotalCpuCost = maxTotalCpuCost;
		this.totalRunningTimeLast24h = totalRunningTimeLast24h;
		this.maxTotalRunningTime = maxTotalRunningTime;
		this.highestPriority = 20000.0f;
	}

	public float getHighestPriority() {
		return highestPriority;
	}

	public void setHighestPriority(float highestPriority) {
		this.highestPriority = highestPriority;
	}

	public int getRunning() {
		return running;
	}

	public void setRunning(int running) {
		this.running = running;
	}

	public float getUserload() {
		return userload;
	}

	public void setUserload(float userload) {
		this.userload = userload;
	}

	public long getMaxTotalRunningTime() {
		return maxTotalRunningTime;
	}

	public void setMaxTotalRunningTime(long maxTotalRunningTime) {
		this.maxTotalRunningTime = maxTotalRunningTime;
	}

	public int getUserId() {
		return userId;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public float getPriority() {
		return priority;
	}

	public void setPriority(float priority) {
		this.priority = priority;
	}

	public int getMaxParallelJobs() {
		return maxParallelJobs;
	}

	public void setMaxParallelJobs(int maxParallelJobs) {
		this.maxParallelJobs = maxParallelJobs;
	}

	public float getComputedPriority() {
		return computedPriority;
	}

	public void setComputedPriority(float computedPriority) {
		this.computedPriority = computedPriority;
	}

	public float getMaxTotalCpuCost() {
		return maxTotalCpuCost;
	}

	public void setMaxTotalCpuCost(float maxTotalCpuCost) {
		this.maxTotalCpuCost = maxTotalCpuCost;
	}

	public long getTotalRunningTimeLast24h() {
		return totalRunningTimeLast24h;
	}

	public void setTotalRunningTimeLast24h(long totalRunningTimeLast24h) {
		this.totalRunningTimeLast24h = totalRunningTimeLast24h;
	}

	public float getTotalCpuCostLast24h() {
		return totalCpuCostLast24h;
	}

	public void setTotalCpuCostLast24h(float totalCpuCostLast24h) {
		this.totalCpuCostLast24h = totalCpuCostLast24h;
	}
}
