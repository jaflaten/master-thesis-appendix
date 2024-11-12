package alien.priority;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
//import alien.optimizers.site.SitequeueReconciler;
//import alien.optimizers.site.SitequeueReconciler;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.Collections.emptyMap;

public class Ptest {
    static final Logger logger = ConfigUtils.getLogger(Ptest.class.getCanonicalName());

    public static void main(String[] args) {

//        mathTest();
        //            algorithmTestRealValues();
//        Map<Integer, PriorityDto> prodData = initDbUsers();
//            boostProdUsers1(prodData);
        //set baseline computedPriority
//        recalculateComputedPriority(prodData);
//        testProdData(prodData);
//        prodSimulation(prodData);
//        prodSimulation(getProdUsers12(), "12");
//        prodSimulation(getSimUsers(), "sim120", 100_000);
//        prodSimulation(getSimUsers(), "sim10k", 10_000);
        prodSimulation(getSimUsers(), "sim1k", 1000);
        prodSimulation(getSimUsers(), "sim100", 100);
        prodSimulation(getProdUsersReal(), "real3008v2", 200_000);
//        prodSimulation(getProdUsers15(), "15");
//        prodSimulation(getProdUsers16(), "16");
//        prodSimulation(getProdUsers17(), "17");
//        prodSimulation(getProdUsers18(), "18");
//        prodSimulation(getProdUsers19(), "19");

//    runOptimizers();
    }



    private static void prodSimulation(Map<Integer, PriorityDto> users, String version, int iterations) {
        int seed = 42;
        System.out.println("Simulating with " + iterations + " iterations and seed 42.");
        List<CoreCostDto> repeatableCoreCosts = getRepeatableCoreCosts(getRepeatableRandomNumberOfCpuCores(seed, iterations));

        List<RunningJobs> runningJobs = computeCpuCoresInUse(users, iterations, repeatableCoreCosts);
        System.out.println("Elements in runningJobs: " + runningJobs.size());
        try {
            writeSimulationToCSV(runningJobs, "prodSimulation" + version + ".csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<RunningJobs> computeCpuCoresInUse(Map<Integer, PriorityDto> users, int maxCpuCoresInUse, List<CoreCostDto> cores) {
        int counterTotalCpuCoresInUse = 0; // job * cpu core usage for that job

        System.out.println("Elements in users: " + users.size());
        List<RunningJobs> runningJobs = new ArrayList<>();

        while (counterTotalCpuCoresInUse < maxCpuCoresInUse) {
            Optional<PriorityDto> priorityDto = users.entrySet().stream()
                    .filter(u -> u.getValue().getComputedPriority() > 0.0)
                    .max(Map.Entry.comparingByValue(Comparator.comparing(PriorityDto::getComputedPriority)))
                    .flatMap(u -> Optional.of(u.getValue()));

            if (priorityDto.isPresent()) {

                PriorityDto user = priorityDto.get();
                if (user.getRunning() < user.getMaxParallelJobs()) {
                    CoreCostDto coreCostDto = cores.get(counterTotalCpuCoresInUse);
                    user.setRunning(user.getRunning() + coreCostDto.getCores());
                    user.setTotalCpuCostLast24h(user.getTotalCpuCostLast24h() + coreCostDto.getCost());
                    counterTotalCpuCoresInUse += coreCostDto.getCores();
                    users.put(user.getUserId(), user);
                }

                PriorityDto dto = priorityDto.get();
                runningJobs.add(
                        new RunningJobs(
                                counterTotalCpuCoresInUse, user.getUserId(), user.getRunning(), dto.getComputedPriority(), dto.getTotalCpuCostLast24h()));

                System.out.println("----- User " + user.getUserId() + " has priority " + dto.getComputedPriority() + " and is running " + user.getRunning() + " jobs -----");
                System.out.println("counter: " + counterTotalCpuCoresInUse + "  max: " + maxCpuCoresInUse);

                recalculateComputedPriority(users);
                System.out.println("Recalculate computed priority finished");

            } else {
                System.out.println("No user with highest priority found");
            }

        }
        System.out.println("Total CPU cores in use to run jobs: " + counterTotalCpuCoresInUse);

        return runningJobs;
    }

    public static boolean haveAllUsersReachedMaxParallelJobs(Map<Integer, PriorityDto> users) {
        for (PriorityDto user : users.values()) {
            if (user.getRunning() < user.getMaxParallelJobs()) {
                return false; // Found a user who has not reached their maxParallelJobs
            }
        }
        return true; // All users have reached their maxParallelJobs
    }

    private static void recalculateComputedPriority(Map<Integer, PriorityDto> users) {
        System.out.println("Users length in recaclulateComputedPriority is: " + users.size());
        for (PriorityDto dto : users.values()) {
            System.out.println("User " + dto.getUserId() + " Before update priority");
            if(dto.getUserId() == 4) {
                System.out.println("dto: " + dto.getUserId() + " running: " + dto.getRunning() + " maxParallelJobs: " + dto.getMaxParallelJobs() + " totalRunningTimeLast24h: " + dto.getTotalRunningTimeLast24h() + " maxTotalRunningTime: " + dto.getMaxTotalRunningTime() + " totalCpuCostLast24h: " + dto.getTotalCpuCostLast24h() + " maxTotalCpuCost: " + dto.getMaxTotalCpuCost());
            }
            CalculateComputedPriority.updateComputedPriority(dto);
            System.out.println("User " + dto.getUserId() + " After update priority");
        }
        System.out.println("completed recalculateComputedPriority");
    }

    private static void runOptimizers() {
//        logger.info("Fooooooooo");
//        setup();
//        PriorityRapidUpdater pru = new PriorityRapidUpdater();
//        pru.start();

//        ActiveUserReconciler aur = new ActiveUserReconciler();
//        aur.start();
//
//        logger.info("Starting PriorityReconciliationService in Ptest");
//        PriorityReconciliationService prs = new PriorityReconciliationService();
//        prs.start();
//
//        SitequeueReconciler sr = new SitequeueReconciler();
//        sr.start();

//        JobAgentUpdater jau = new JobAgentUpdater();
//        jau.start();
//
//        InactiveJobHandler ijh = new InactiveJobHandler();
//        ijh.start();

//        logger.info("Starting CheckJobStatus in Ptest");
//        CheckJobStatus cjs = new CheckJobStatus();
//        cjs.start();

//        OldJobRemover ojr = new OldJobRemover();
//        ojr.start();

//        OverwaitingJobHandler ojh = new OverwaitingJobHandler();
//        ojh.start();

    }

    private static void algorithmTestRealValues() throws IOException {
        execProdUsers1();
//        execProdUsers2();
    }

    protected static void boostProdUsers1(Map<Integer, PriorityDto> prodData) throws IOException {
        System.out.println("Boost values for set of users ProdUsers1: ");
        for (PriorityDto dto : prodData.values()) {
            int boostValue = CalculateComputedPriority.findBoostValue(dto);
//            System.out.println("Initial user values: userId: " + dto.getUserId() + " computedPriority: " + dto.getComputedPriority() + " boostValue: " + boostValue);
            System.out.println("UserId: " + dto.getUserId() + " Boost value: " + boostValue);
        }
//        writeToCSV(prodUsers1, "prodUsers1_boosted.csv");
    }

    private static void testProdData(Map<Integer, PriorityDto> prodData) {
        for (PriorityDto dto : prodData.values()) {
            System.out.println("Initial values: User Id: " + dto.getUserId() + ", User Load: " + dto.getUserload() + ", Running: " + dto.getRunning() + ", Max Parallel Jobs: " + dto.getMaxParallelJobs() + ", Total Running Time Last 24h: " + dto.getTotalRunningTimeLast24h() + ", Max Total Running Time: " + dto.getMaxTotalRunningTime() + ", Priority: " + dto.getPriority() + ", Total CPU Cost Last 24h: " + dto.getTotalCpuCostLast24h() + ", Max Total CPU Cost: " + dto.getMaxTotalCpuCost());
            CalculateComputedPriority.updateComputedPriority(dto);
        }
    }

    public static List<Integer> getRepeatableRandomNumberOfCpuCores(int seed, int iterations) {
        Random random = new Random(seed);
        List<Integer> cores = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            int number = (1 + random.nextInt(8));
            if (number == 1 || number == 2 || number == 4 || number == 6 || number == 8)
                cores.add(number);
            else
                cores.add(2 * number);
        }
        return cores;
    }

    private static List<CoreCostDto> getRepeatableCoreCosts(List<Integer> cores) {
        int walltime = 1 * 3600;
        int price = 1;
        List<CoreCostDto> coreCosts = new ArrayList<>();

        for (int core : cores) {
            float cost = (core * walltime) * price;
            coreCosts.add(new CoreCostDto(core, cost));
        }
        System.out.println("The size of coreCosts is: " + coreCosts.size());

        return coreCosts;
    }

    private static void execProdUsers1() throws IOException {
        List<PriorityDto> prodUsers1 = getProdUsers1();
        writeToCSV(prodUsers1, "prodUsers1.csv");
        System.out.println("Input data written to prodUsers1.csv");

        for (PriorityDto dto : prodUsers1) {
            CalculateComputedPriority.updateComputedPriority(dto);
        }
        writeToCSV(prodUsers1, "prodUsers1_updated.csv");
        System.out.println("Updated data written to prodUsers1_updated.csv");

        List<PriorityDto> expProdUsers1 = getProdUsers1();
        writeToCSV(expProdUsers1, "expProdUsers1.csv");
        System.out.println("Input data written to expProdUsers1.csv \n");

        for (PriorityDto dto : expProdUsers1) {
            CalculateComputedPriority.updateComputedPriority(dto);
        }

        writeToCSV(expProdUsers1, "expProdUsers1_updated.csv");
        System.out.println("Updated data written to expProdUsers1_updated.csv");
    }

//    private static void execProdUsers2() throws IOException {
//        Map<Integer, PriorityDto> prodUsers2 = getProdUsers2();
//        writeToCSV(prodUsers2, "prodUsers2.csv");
//        System.out.println("Input data written to prodUsers2.csv");
//
//        for (PriorityDto dto : prodUsers2) {
//            CalculateComputedPriority.updateComputedPriority(dto);
//        }
//        writeToCSV(prodUsers2, "prodUsers2_updated.csv");
//        System.out.println("Calculated by original algorithm -  written to prodUsers2_updated.csv \n");
//
//        Map<Integer, PriorityDto> expProdUsers2 = getProdUsers2();
//        for (PriorityDto dto : expProdUsers2) {
//            CalculateComputedPriority.updateComputedPriorityExperimental(dto);
//        }
//        writeToCSV(expProdUsers2, "expProdUsers2.csv");
//        System.out.println("Calculated by experimental algorithm -  written to expProdUsers2.csv  \n");
//    }

    static final Monitor monitor = MonitorFactory.getMonitor(Ptest.class.getCanonicalName());

    private static Map<Integer, PriorityDto> initDbUsers() {
        System.out.println("Attempting to get a DB connection...");
        try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
            if (db == null) {
                System.out.println("Ptest could not get a DB connection - is the tunnel open?");
                return emptyMap();
            }

            System.out.println("Db connection established");
            db.setQueryTimeout(60);

            String q = "SELECT userId, priority, running, maxParallelJobs, totalRunningTimeLast24h, maxTotalRunningTime, totalCpuCostLast24h, maxTotalCpuCost from PRIORITY where userId in (974855, 1234576, 1234578, 1235889)";


            Map<Integer, PriorityDto> dtos = new HashMap<>();
            try (Timing t = new Timing(monitor, "calculateComputedPriority")) {
                logger.log(Level.INFO, "Calculating computed priority");
                db.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                db.query(q);

                System.out.println("Query executed successfully. Processing results...");
                while (db.moveNext()) {
                    Integer userId = Integer.valueOf(db.geti("userId"));
                    dtos.computeIfAbsent(
                            userId,
                            k -> new PriorityDto(db));
                }
            } catch (Exception e) {
                System.out.println("Exception thrown while calculating computedPriority: \n" + e);
                ;
            }
            return dtos;
        } catch (Exception e) {
            System.out.println("Error getting DB connection: \n" + e);
            return emptyMap();
        }
    }

    private static void mathTest() {
        System.out.println("exp of -0.01 = " + Math.exp(-0.01));
        System.out.println("exp of 0.01 = " + Math.exp(0.01));
        System.out.println("exp of -0.02 = " + Math.exp(-0.02));
        System.out.println("exp of 0.02 = " + Math.exp(0.02));
        System.out.println("exp of -0.05 = " + Math.exp(-0.05));
        System.out.println("exp of 0.05 = " + Math.exp(0.05));
        System.out.println("exp of -0.1 = " + Math.exp(-0.1));
        System.out.println("exp of 0.1 = " + Math.exp(0.1));
        System.out.println("exp of -0.2 = " + Math.exp(-0.2));
        System.out.println("exp of 0.2 = " + Math.exp(0.2));
        System.out.println("exp of -0.4 = " + Math.exp(-0.4));
        System.out.println("exp of 0.4 = " + Math.exp(0.4));
        System.out.println("exp of -0.6 = " + Math.exp(-0.6));
        System.out.println("exp of 0.6 = " + Math.exp(0.6));
        System.out.println("exp of -0.8 = " + Math.exp(-0.8));
        System.out.println("exp of 0.8 = " + Math.exp(0.8));
        System.out.println("exp of 0 = " + Math.exp(0));
        System.out.println("exp of 1 = " + Math.exp(1));
        System.out.println("exp of 0.025 = " + Math.exp(0.025));
        System.out.println("exp of -0.025 = " + Math.exp(-0.025));
        System.out.println("exp of 0.0005 = " + Math.exp(0.0005));
        System.out.println("exp of 0.00005 = " + Math.exp(0.00005));
        System.out.println("exp of -0.0005 = " + Math.exp(-0.0005));
        System.out.println("exp of -0.00005 = " + Math.exp(-0.00005));


        System.out.println("LOG of 0.01 = " + Math.log(0.01));
        System.out.println("LOG of 0.02 = " + Math.log(0.02));
        System.out.println("LOG of 0.05 = " + Math.log(0.05));
        System.out.println("LOG of 0.1 = " + Math.log(0.1));
        System.out.println("LOG of 0.2 = " + Math.log(0.2));
        System.out.println("LOG of 0.4 = " + Math.log(0.4));
        System.out.println("LOG of 0.6 = " + Math.log(0.6));
        System.out.println("LOG of 0.8 = " + Math.log(0.8));
        System.out.println("LOG of 0 = " + Math.log(0));
        System.out.println("LOG of 1 = " + Math.log(1));
        System.out.println("LOG of 0.025 = " + Math.log(0.025));
        System.out.println("LOG of 0.0005 = " + Math.log(0.0005));
        System.out.println("LOG of 0.00005 = " + Math.log(0.00005));
    }

    private static void setup() {
        //test code
        PriorityRegister.JobCounter.getCounterForUser(1).addCost(1);
        PriorityRegister.JobCounter.getCounterForUser(1).addCputime(1);
        PriorityRegister.JobCounter.getCounterForUser(1).addCputime(3);
        PriorityRegister.JobCounter.getCounterForUser(1).incWaiting();
        PriorityRegister.JobCounter.getCounterForUser(1).incRunningAndDecWaiting(1);
        PriorityRegister.JobCounter.getCounterForUser(2).addCost(1);
        PriorityRegister.JobCounter.getCounterForUser(2).addCputime(1);
        PriorityRegister.JobCounter.getCounterForUser(2).incWaiting();
        PriorityRegister.JobCounter.getCounterForUser(2).incRunningAndDecWaiting(1);
        PriorityRegister.JobCounter.getCounterForUser(2).incRunning(8);
        PriorityRegister.JobCounter.getCounterForUser(2).incRunning(8);
        logger.info("user 2 running jobs: " + String.valueOf(PriorityRegister.JobCounter.getCounterForUser(2).getRunning()));
        PriorityRegister.JobCounter.getCounterForUser(2).incRunning(8);
        logger.info("user 2 running jobs: " + String.valueOf(PriorityRegister.JobCounter.getCounterForUser(2).getRunning()));
        PriorityRegister.JobCounter.getCounterForUser(2).decRunning(8);
        logger.info("decreased by 8 - user 2 running jobs: " + String.valueOf(PriorityRegister.JobCounter.getCounterForUser(2).getRunning()));
        PriorityRegister.JobCounter.getCounterForUser(2).decRunning(-6);
        logger.info("decreased by -6 - user 2 running jobs: " + String.valueOf(PriorityRegister.JobCounter.getCounterForUser(2).getRunning()));
        logger.info("user 1 running jobs: " + String.valueOf(PriorityRegister.JobCounter.getCounterForUser(1).getRunning()));

        PriorityRegister.JobCounter.getCounterForUser(3).decRunning(-6);
        PriorityRegister.JobCounter.getCounterForUser(3).decRunning(-6);
        PriorityRegister.JobCounter.getCounterForUser(3).decRunning(-6);
        logger.info("user 3 running jobs: " + String.valueOf(PriorityRegister.JobCounter.getCounterForUser(3).getRunning()));

        //Teste på setJobStatus i staden
        newUsers();
    }

    private static void newUsers() {
        // New users setup
        PriorityRegister.JobCounter.getCounterForUser(4).addCost(2);
        PriorityRegister.JobCounter.getCounterForUser(4).addCputime(4);
        PriorityRegister.JobCounter.getCounterForUser(4).incWaiting();
        PriorityRegister.JobCounter.getCounterForUser(4).incRunningAndDecWaiting(2);

        PriorityRegister.JobCounter.getCounterForUser(5).addCost(3);
        PriorityRegister.JobCounter.getCounterForUser(5).addCputime(2);
        PriorityRegister.JobCounter.getCounterForUser(5).incWaiting();
        PriorityRegister.JobCounter.getCounterForUser(5).incRunningAndDecWaiting(3);
        PriorityRegister.JobCounter.getCounterForUser(5).incRunning(5);

        PriorityRegister.JobCounter.getCounterForUser(6).addCost(5);
        PriorityRegister.JobCounter.getCounterForUser(6).addCputime(5);
        PriorityRegister.JobCounter.getCounterForUser(6).incWaiting();
        PriorityRegister.JobCounter.getCounterForUser(6).incRunning(10);
        PriorityRegister.JobCounter.getCounterForUser(6).decRunning(5);

        PriorityRegister.JobCounter.getCounterForUser(7).addCost(4);
        PriorityRegister.JobCounter.getCounterForUser(7).addCputime(1);
        PriorityRegister.JobCounter.getCounterForUser(7).incWaiting();
        PriorityRegister.JobCounter.getCounterForUser(7).incRunning(2);
        PriorityRegister.JobCounter.getCounterForUser(7).decRunning(-3);

        PriorityRegister.JobCounter.getCounterForUser(8).addCost(1);
        PriorityRegister.JobCounter.getCounterForUser(8).addCputime(6);
        PriorityRegister.JobCounter.getCounterForUser(8).incWaiting();
        PriorityRegister.JobCounter.getCounterForUser(8).incRunning(4);
        PriorityRegister.JobCounter.getCounterForUser(8).decRunning(1);

        PriorityRegister.JobCounter.getCounterForUser(9).addCost(2);
        PriorityRegister.JobCounter.getCounterForUser(9).addCputime(3);
        PriorityRegister.JobCounter.getCounterForUser(9).incWaiting();
        PriorityRegister.JobCounter.getCounterForUser(9).incRunningAndDecWaiting(4);
        PriorityRegister.JobCounter.getCounterForUser(9).incRunning(6);
        PriorityRegister.JobCounter.getCounterForUser(9).decRunning(2);

        PriorityRegister.JobCounter.getCounterForUser(10).addCost(6);
        PriorityRegister.JobCounter.getCounterForUser(10).addCputime(7);
        PriorityRegister.JobCounter.getCounterForUser(10).incWaiting();
        PriorityRegister.JobCounter.getCounterForUser(10).incRunning(7);
        PriorityRegister.JobCounter.getCounterForUser(10).decRunning(30);

        // Logging running jobs for new users
        for (int userId = 4; userId <= 10; userId++) {
            logger.info("user " + userId + " running jobs: " +
                    String.valueOf(PriorityRegister.JobCounter.getCounterForUser(userId).getRunning()));
        }
    }


    // testing algorithm

    public static List<PriorityDto> getProdUsers1() {
        return new ArrayList<>(List.of(
                new PriorityDto(974855, 10.0f, 200000, 0.082065f, 1000000000000L, 16342, 0.0584273f, 100000000000000.0F, 7067049650L, 10000.0f),
                new PriorityDto(1234576, 20000.0f, 60000, 0.500733f, 1000000000000L, 30044, 49.9024f, 100000000000000.0F, 278973142L, 5000.0f),
                new PriorityDto(1234578, 12000.0f, 50000, 0.18894f, 1000000000000L, 9918, 115.024f, 100000000000000.0F, 338739306L, 500000.0f),
                new PriorityDto(1235889, 12000.0f, 85000, 0.159882f, 100000000000000L, 13442, 81.2414f, 100000000000000.0F, 785199699L, 12000000.0f)

        ));
    }


    public static Map<Integer, PriorityDto> getSimUsers() {
        HashMap<Integer, PriorityDto> users = new HashMap<>();
        users.put(0, new PriorityDto(0, 700.0f, 20000, 0.58171f, 10000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1, new PriorityDto(1, 1000.0f, 20000, 0.8340667f, 10000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(2, new PriorityDto(2, 500.0f, 20000, 0.07836f, 10000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(3, new PriorityDto(3, 1100.0f, 20000, 0.7816706f, 10000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(4, new PriorityDto(4, 900.0F, 20000, 0.7816706f, 10000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(5, new PriorityDto(5, 800.0F, 20000, 0.7816706f, 10000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));

        return users;
    }

    public static Map<Integer, PriorityDto> getProdUsers12() {
        HashMap<Integer, PriorityDto> users = new HashMap<>();
        users.put(974855, new PriorityDto(974855, 10.0f, 200000, 0.58171f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1234576, new PriorityDto(1234576, 20000.0f, 60000, 0.8340667f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1234578, new PriorityDto(1234578, 12000.0f, 50000, 0.07836f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1235889, new PriorityDto(1235889, 12000.0f, 85000, 0.7816706f, 100000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1235997, new PriorityDto(1235997, 1.0F, 1000, 0.7816706f, 10000000L, 0, 0.99f, 1000000000.0F, 0L, 0.0f));

        return users;
    }

    public static Map<Integer, PriorityDto> getProdUsersReal() {
        HashMap<Integer, PriorityDto> users = new HashMap<>();
        users.put(974855, new PriorityDto(974855, 2.0f, 200_000, 0.58171f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        //users.put(1234576, new PriorityDto(1234576, 1_000_000.0f, 80000, 0.8340667f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1234578, new PriorityDto(1234578, 12000.0f, 50000, 0.07836f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1235889, new PriorityDto(1235889, 12000.0f, 85000, 0.7816706f, 100000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));

        return users;
    }

    // only difference from prodUsers1 is that the running jobs are much higher. Userload and computedPriority is updated to reflect this - Runningtime is the same.
    public static Map<Integer, PriorityDto> getProdUsers15() {
        HashMap<Integer, PriorityDto> users = new HashMap<>();
        users.put(974855, new PriorityDto(974855, 10.0f, 200000, 0.58171f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1234576, new PriorityDto(1234576, 20000.0f, 60000, 0.8340667f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1234578, new PriorityDto(1234578, 15000.0f, 50000, 0.07836f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1235889, new PriorityDto(1235889, 15000.0f, 85000, 0.7816706f, 100000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1235997, new PriorityDto(1235997, 1.0F, 1000, 0.7816706f, 10000000L, 0, 0.99f, 1000000000.0F, 0L, 0.0f));

        return users;
    }
        // 20k and 15k try testing priority , add another random user, play with priorities until this user given max parallel jobs still get to run some jobs


    private static Map<Integer, PriorityDto> getProdUsers16() {
        HashMap<Integer, PriorityDto> users = new HashMap<>();
        users.put(974855, new PriorityDto(974855, 10.0f, 200000, 0.58171f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1234576, new PriorityDto(1234576, 20000.0f, 60000, 0.8340667f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1234578, new PriorityDto(1234578, 16000.0f, 50000, 0.07836f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1235889, new PriorityDto(1235889, 16000.0f, 85000, 0.7816706f, 100000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1235997, new PriorityDto(1235997, 1.0F, 1000, 0.7816706f, 10000000L, 0, 0.99f, 1000000000.0F, 0L, 0.0f));

        return users;
    }

    private static Map<Integer, PriorityDto> getProdUsers17() {
        HashMap<Integer, PriorityDto> users = new HashMap<>();
        users.put(974855, new PriorityDto(974855, 10.0f, 200000, 0.58171f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1234576, new PriorityDto(1234576, 20000.0f, 60000, 0.8340667f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1234578, new PriorityDto(1234578, 17000.0f, 50000, 0.07836f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1235889, new PriorityDto(1235889, 17000.0f, 85000, 0.7816706f, 100000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1235997, new PriorityDto(1235997, 1.0F, 1000, 0.7816706f, 10000000L, 0, 0.99f, 1000000000.0F, 0L, 0.0f));

        return users;
    }


    private static Map<Integer, PriorityDto> getProdUsers18() {
        HashMap<Integer, PriorityDto> users = new HashMap<>();
        users.put(974855, new PriorityDto(974855, 10.0f, 200000, 0.58171f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1234576, new PriorityDto(1234576, 20000.0f, 60000, 0.8340667f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1234578, new PriorityDto(1234578, 18000.0f, 50000, 0.07836f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1235889, new PriorityDto(1235889, 18000.0f, 85000, 0.7816706f, 100000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1235997, new PriorityDto(1235997, 1.0F, 1000, 0.7816706f, 10000000L, 0, 0.99f, 1000000000.0F, 0L, 0.0f));

        return users;
    }

    private static Map<Integer, PriorityDto> getProdUsers19() {
        HashMap<Integer, PriorityDto> users = new HashMap<>();
        users.put(974855, new PriorityDto(974855, 10.0f, 200000, 0.58171f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1234576, new PriorityDto(1234576, 20000.0f, 60000, 0.8340667f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1234578, new PriorityDto(1234578, 19000.0f, 50000, 0.07836f, 1000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1235889, new PriorityDto(1235889, 19000.0f, 85000, 0.7816706f, 100000000000000L, 0, 0.99f, 100000000000000.0F, 0L, 0.0f));
        users.put(1235997, new PriorityDto(1235997, 1.0F, 1000, 0.7816706f, 10000000L, 0, 0.99f, 1000000000.0F, 0L, 0.0f));

        return users;
    }

    public static void writeToCSV(List<PriorityDto> priorityDtos, String filename) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            // Writing header
            writer.write("Userload, userId, activeCores, maxParallelCores, totalRunningTimeLast24h, maxTotalRunningtimeLast24h, priority, computedPriority, totalCpuCost24h, maxTotalCpuCost\n");

            // Writing data
            for (PriorityDto dto : priorityDtos) {
                writer.write(dto.getUserload() + "," +
                        dto.getUserId() + "," +
                        dto.getRunning() + "," +
                        dto.getMaxParallelJobs() + "," +
                        dto.getTotalRunningTimeLast24h() + "," +
                        dto.getMaxTotalRunningTime() + "," +
                        dto.getPriority() + "," +
                        dto.getComputedPriority() + "," +
                        dto.getTotalCpuCostLast24h() + "," +
                        dto.getMaxTotalCpuCost() + "\n");
            }
        }
    }

    public static void writeSimulationToCSV(List<RunningJobs> runningJobs, String filename) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            writer.write("Total CPU core usage (Jobs),User ID,User CPU Core usage,Computed Priority start of Job,userCurrentCost\n");

            for (RunningJobs job : runningJobs) {
                writer.write(job.getCounterTotalRunningJobs() + "," +
                        job.getUserId() + "," +
                        job.getUserCurrentlyRunningJobs() + "," +
                        job.getComputedPriorityAtJobStart() + ", " +
                        job.getCurrentUserCost() + "\n");
            }
        }
    }
}
