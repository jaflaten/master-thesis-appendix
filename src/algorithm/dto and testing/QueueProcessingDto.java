package alien.priority;

import alien.taskQueue.JobStatus;

/**
 * @author Jorn-Are Flaten
 * @since 2023-12-04
 */
public class QueueProcessingDto {
	private final int userId;
	private double cost;
	private long cputime;
	private int cpucores;
	private int statusId;
	private int waiting;

	/**
	 * @param userId
	 */
	public QueueProcessingDto(int userId) {
		this.userId = userId;
		this.cost = 0;
		this.cputime = 0;
		this.cpucores = 0;
		this.statusId = 0;
		this.waiting = 0;
	}

	/**
	 * @return Waiting jobs for the user
	 */
	public int getWaiting() {
		return waiting;
	}

	/**
	 * @return CPU cores used by job
	 */
	public int getCpucores() {
		return cpucores;
	}

	/**
	 * @return StatusId
	 */
	public int getStatusId() {
		return statusId;
	}

	/**
	 * @return User ID
	 */
	public int getUserId() {
		return userId;
	}

	/**
	 * @return accumulated cost
	 */
	public double getCost() {
		return cost;
	}

	/**
	 * @return accumulated CPU time
	 */
	public long getCputime() {
		return cputime;
	}

	/**
	 * @param jobCost
	 * @param cpuTime
	 * @param ncpucores
	 * @param nstatusId
	 */
	public void addAccounting(double jobCost, long cpuTime, int ncpucores, int nstatusId) {
		this.cost += jobCost;
		this.cputime += cpuTime;
		this.cpucores += getCoresForRunningStates(ncpucores, nstatusId);
		this.waiting += getJobsInWaitingState(nstatusId);
	}

	private static int getCoresForRunningStates(int cpucores, int statusId) {
		return JobStatus.runningStates().contains(JobStatus.getStatusByAlien(Integer.valueOf(statusId))) ? cpucores : 0;
	}

	private static int getJobsInWaitingState(int statusId) {
		if (JobStatus.WAITING.getAliEnLevel() == statusId)
			return 1;

		return 0;
	}

}
