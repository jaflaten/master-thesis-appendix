package alien.optimizers.priority;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Jorn-Are Flaten
 * @since 2023-12-08
 */
public class JobAgentUpdater extends Optimizer {
	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(JobAgentUpdater.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(JobAgentUpdater.class.getCanonicalName());

	@Override
	public void run() {
		logger.log(Level.INFO, "JobAgentUpdater starting");
		this.setSleepPeriod(60 * 5 * 1000); // 5m
		int frequency = (int) this.getSleepPeriod();

		while (true) {
			try {
				final boolean updated = DBSyncUtils.updatePeriodic(frequency, JobAgentUpdater.class.getCanonicalName(), this);
				if (updated) {
					updateComputedPriority();
				}
			}
			catch (Exception e) {
				try {
					logger.log(Level.SEVERE, "Exception executing optimizer", e);
					DBSyncUtils.registerException(JobAgentUpdater.class.getCanonicalName(), e);
				}
				catch (Exception e2) {
					logger.log(Level.SEVERE, "Cannot register exception in the database", e2);
				}
			}

			try {
				logger.log(Level.INFO, "JobAgentUpdater sleeping for " + this.getSleepPeriod() + " ms");
				sleep(this.getSleepPeriod());
			}
			catch (InterruptedException e) {
				logger.log(Level.SEVERE, "JobAgentUpdater interrupted", e);
			}
		}
	}

	private static void updateComputedPriority() {
		try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
			if (db == null) {
				logger.log(Level.SEVERE, "JobAgentUpdater could not get a DB connection");
				return;
			}

			db.setQueryTimeout(60);

			String s = "UPDATE JOBAGENT INNER JOIN PRIORITY USING(userId) SET JOBAGENT.priority = PRIORITY.computedPriority";
			try (Timing t = new Timing(monitor, "JobAgentUpdater")) {
				logger.log(Level.INFO, "2-JobAgentUpdater starting to update priority in JOBAGENT table");
				t.startTiming();
				db.query(s);
				t.endTiming();
				logger.log(Level.INFO, "JobAgentUpdater finished updating JOBAGENT table, took " + t.getMillis() + " ms");

				String registerLog = "Finished updating JOBAGENT table priority values, in " + t.getMillis() + " ms\n";
				DBSyncUtils.registerLog(JobAgentUpdater.class.getCanonicalName(), registerLog);
			}
		}
	}

}
