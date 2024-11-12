package alien.optimizers.priority;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import alien.priority.CalculateComputedPriority;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

import java.sql.Connection;
import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Jorn-Are Flaten
 * @since 2024-29-02
 */
public class ActiveUserReconciler extends Optimizer {

	static final Logger logger = ConfigUtils.getLogger(ActiveUserReconciler.class.getCanonicalName());

	static final Monitor monitor = MonitorFactory.getMonitor(ActiveUserReconciler.class.getCanonicalName());

	@Override
	public void run() {
		this.setSleepPeriod(Duration.ofMinutes(5).toMillis());
		int frequency = (int) this.getSleepPeriod();

		while (true) {
			logger.log(Level.INFO, "ActiveUserReconciler starting");
			final boolean updated = DBSyncUtils.updatePeriodic(frequency, ActiveUserReconciler.class.getCanonicalName(), this);
			try {
				if (updated) {
					reconcileActiveUsers();
				}
			}
			catch (Exception e) {
				try {
					logger.log(Level.SEVERE, "Exception executing optimizer", e);
					DBSyncUtils.registerException(ActiveUserReconciler.class.getCanonicalName(), e);
				}
				catch (Exception e2) {
					logger.log(Level.SEVERE, "Cannot register exception in the database", e2);
				}
			}

			try {
				logger.log(Level.INFO, "ActiveUserReconciler sleeps " + this.getSleepPeriod() + " ms");
				sleep(this.getSleepPeriod());
			}
			catch (InterruptedException e) {
				logger.log(Level.WARNING, "ActiveUserReconciler interrupted", e);
			}
		}
	}

	private static void reconcileActiveUsers() {
		try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
			if (db == null) {
				logger.log(Level.SEVERE, "ActiveUserReconciler could not get a DB connection");
				return;
			}

			StringBuilder registerLog = new StringBuilder();
			try (Timing t = new Timing(monitor, "TQ_reconcile_ActiveUserReconciler_ms")) {
				CalculateComputedPriority.updateComputedPriority(true);
				registerLog.append("Updated computed priority for active users in ")
						.append(t.getMillis())
						.append(" ms\n");
			}
			db.setQueryTimeout(60);
			db.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			getInactiveUsers(db, registerLog);
			updateActivityFlag(db, registerLog);
			DBSyncUtils.registerLog(ActiveUserReconciler.class.getCanonicalName(), registerLog.toString());
		}
	}

	private static void updateActivityFlag(DBFunctions db, StringBuilder registerLog) {
		try (Timing t = new Timing(monitor, "TQ_reconcile_updateInactiveUsers_ms")) {
			t.startTiming();
			int counter = 0;
			while (db.moveNext()) {
				int userId = db.geti("userId");
				String updateQuery = "UPDATE PRIORITY SET active = 0 WHERE userId = " + userId;
				logger.log(Level.INFO, "Setting user " + userId + " to inactive");
				db.query(updateQuery);
				counter++;
			}

			logger.log(Level.INFO, "Updated " + counter + " inactive users");
			t.endTiming();
			logger.log(Level.INFO, "Updating inactive users took " + t.getMillis() + " ms");
			registerLog.append("Updated ")
					.append(counter)
					.append(" inactive users in ")
					.append(t.getMillis())
					.append(" ms\n");
		}

	}

	private static void getInactiveUsers(DBFunctions db, StringBuilder registerLog) {
		try (Timing t = new Timing(monitor, "TQ_reconcile_getInactiveUsers_ms")) {
			t.startTiming();
			String q = "SELECT userId from PRIORITY where totalRunningTimeLast24h = 0 AND active = 1";
			db.query(q);
			t.endTiming();
			logger.log(Level.INFO, "Retrieving inactive users took " + t.getMillis() + " ms");
			registerLog.append("Retrieving inactive users in ")
					.append(t.getMillis())
					.append(" ms\n");
		}
	}
}
