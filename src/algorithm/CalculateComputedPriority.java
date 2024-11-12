package alien.priority;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.DBSyncUtils;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.String.valueOf;

/**
 * @author Jorn-Are Flaten
 * @since 2023-12-04
 */
public class CalculateComputedPriority {
	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(CalculateComputedPriority.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(CalculateComputedPriority.class.getCanonicalName());

	/**
	 * Update the computed priority for users
	 *
	 * @param onlyActiveUsers
	 */
	public static void updateComputedPriority(boolean onlyActiveUsers) {
		StringBuilder registerLog = new StringBuilder();
		try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
			if (db == null) {
				logger.log(Level.SEVERE, "CalculatePriority could not get a DB connection");
				return;
			}

			db.setQueryTimeout(60);

			String q;
			if (onlyActiveUsers) {
				q = "SELECT *, (SELECT MAX(priority) FROM PRIORITY) AS highestPriority from PRIORITY where totalRunningTimeLast24h > 0";
			}
			else {
				q = "SELECT *, (SELECT MAX(priority) FROM PRIORITY) AS highestPriority from PRIORITY";
			}

			Map<Integer, PriorityDto> dtos = new HashMap<>();
			try (Timing t = new Timing(monitor, "calculateComputedPriority")) {
				logger.log(Level.INFO, "Calculating computed priority");
				db.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
				db.query(q);

				while (db.moveNext()) {
					Integer userId = Integer.valueOf(db.geti("userId"));
					dtos.computeIfAbsent(
							userId,
							k -> new PriorityDto(db));

					updateComputedPriority(dtos.get(userId));
				}
				registerLog.append("Calculating computed priority for ")
						.append(dtos.size())
						.append(" users.\n")
						.append(" Updated userload and computedPriority values will be written to the PRIORITY table in processesdev DB.\n ");

				logger.log(Level.INFO, "Finished calculating, preparing to update " + dtos.size() + " elements in the PRIORITY table...");
				executeUpdateQuery(db, dtos, registerLog);

				DBSyncUtils.registerLog(CalculateComputedPriority.class.getCanonicalName(), registerLog.toString());
			}
			catch (Exception e) {
				logger.log(Level.SEVERE, "Exception thrown while calculating computedPriority", e);
			}
		}
	}

	private static void executeUpdateQuery(DBFunctions db, Map<Integer, PriorityDto> dtos, StringBuilder registerLog) {
		try (Timing t = new Timing(monitor, "TQ_update_computed_priority_ms")) {
			dtos.forEach((id, dto) -> {
				try (Timing t2 = new Timing(monitor, "TQ_single_row_update_ms")) {
					String query = "UPDATE PRIORITY SET userload = ?, computedPriority = ? WHERE userId = ?;";
					db.query(query, false, Float.valueOf(dto.getUserload()), Float.valueOf(dto.getComputedPriority()), id);
					t2.endTiming();
					logger.log(Level.INFO, "Updating PRIORITY row for user " + id + " completed in " + t2.getMillis() + " ms");
				}
			});

			t.endTiming();
			logger.log(Level.INFO, "Finished updating PRIORITY table row by row, took " + t.getMillis() + " ms");
			registerLog.append("Updating PRIORITY table row by row completed in ").append(t.getMillis()).append(" ms\n");
		}
	}

	private static void updateComputedPriority(PriorityDto dto) {
		if (isQuotaExceeded(dto)) {
			return;
		}

		float maxBaselinePriority = dto.getHighestPriority();
		float costWeight = 0.1f;
		float activeCpuCoresWeight = 0.5f;
		float priorityWeight = 0.4f;

		float costQuotient = (float) Math.sqrt(dto.getTotalCpuCostLast24h() / dto.getMaxTotalCpuCost());
		float normalizedRunningQuotient = (float) dto.getRunning() / dto.getMaxParallelJobs();
		float normalizedPriority = 1 - (dto.getPriority() / maxBaselinePriority);

		float weightedSum = costWeight * costQuotient + activeCpuCoresWeight * normalizedRunningQuotient + priorityWeight * normalizedPriority;

		float computedPriority = (weightedSum + findBoostValue(dto));

		System.out.println("ComputedPriority: " + Math.abs(computedPriority - 1) + " for user: " + dto.getUserId() + " , weightedSum = " + weightedSum + " , costQuotient = " + costQuotient +
				" , and running = " + dto.getRunning() + " out of maximum = " + dto.getMaxParallelJobs() + "\n");

		dto.setComputedPriority(Math.abs(computedPriority - 1));

	}

	private static int findBoostValue(PriorityDto dto) {
		int noBoost = 0;
		int maxBoost = 10;
		int minCost = 100_000;
		int maxCost = 1_000_000;

		if (dto.getTotalCpuCostLast24h() > maxCost) {
			return noBoost;
		}

		if (dto.getTotalCpuCostLast24h() < minCost) {
			return maxBoost;
		}

		// Linear interpolation to calculate the boost value
		double slope = (double) (noBoost - maxBoost) / (maxCost - minCost);
		return (int) Math.round(maxBoost + slope * (dto.getTotalCpuCostLast24h() - minCost));
	}

    private static boolean isQuotaExceeded(PriorityDto dto) {
        if (dto.getTotalRunningTimeLast24h() > dto.getMaxTotalRunningTime()) {
            dto.setComputedPriority(-1);
            registerLog(dto.getUserId(), "TotalRunningTimeLast24h", valueOf(dto.getTotalRunningTimeLast24h()), valueOf(dto.getMaxTotalRunningTime()));
            return true;
        }
        if (dto.getRunning() > dto.getMaxParallelJobs()) {
            dto.setComputedPriority(-1);
            registerLog(dto.getUserId(), "Active cores", valueOf(dto.getRunning()), valueOf(dto.getMaxParallelJobs()));
            return true;
        }
        if (dto.getTotalCpuCostLast24h() > dto.getMaxTotalCpuCost()) {
            dto.setComputedPriority(-1);
            registerLog(dto.getUserId(), "TotalCpuCostLast24h", valueOf(dto.getTotalCpuCostLast24h()), valueOf(dto.getMaxTotalCpuCost()));
            return true;
        }

        return false;
    }

    private static void registerLog(int userId, String type, String min, String max) {
        String log = "User " + userId + " = " + TaskQueueUtils.getUser(userId) + " has exceeded quota for " + type + " with value " + min + " > " + max;
        logger.log(Level.INFO, log);
        DBSyncUtils.registerLog(CalculateComputedPriority.class.getCanonicalName(), log);
    }
}
