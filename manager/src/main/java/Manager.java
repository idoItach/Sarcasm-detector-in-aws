import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import software.amazon.awssdk.services.sqs.model.Message;

public class Manager {

    final static AWS aws = AWS.getInstance();
    private static volatile Manager managerInstance = getManagerInstance();

    public static final int MAX_WORKERS_INSTANCES = 8; // not supposed to run more than 9 instances (1 for manager)
    public static final int NUMBER_OF_LOCAL_APPLICATION_THREADS = 3;
    public static final int NUMBER_OF_WORKERS_THREADS = 3;

    private final AtomicInteger numberOfRequiredWorkers = new AtomicInteger(0);
    private final AtomicInteger numberOfActivateWorker = new AtomicInteger(0);
    private final AtomicBoolean managerShouldTerminate = new AtomicBoolean(false);

    private ConcurrentLinkedQueue<Message> localApplicationTasksQueue; // queue for new tasks requests comes from local
                                                                       // applications
    private ConcurrentLinkedQueue<Message> workersTasksQueue; // queue for finished messages comes from workers

    // next line keeps objects: <localApplicationId + aws.delimiter + jobIndex ,
    // LocalApplicationTaskInfo>
    private ConcurrentHashMap<String, LocalApplicationTaskInfo> localApplicationsTasksTracker;
    // Data>

    final private Object localApplicationTasksLock = new Object();
    final private Object workersTasksLock = new Object();

    // private constructor - will called only once
    private Manager() {
        System.out.println("[DEBUG] create new manager");
    }

    public static Manager getManagerInstance() {
        if (managerInstance == null) {
            synchronized (Manager.class) {
                if (managerInstance == null) {
                    System.out.println("[DEBUG] getManagerInstance called");
                    managerInstance = new Manager();
                }
            }
        }
        return managerInstance;
    }

    public static void main(String[] args) {
        System.out.println("[DEBUG] manager starts");
        managerInstance.initializeQueuesAndMaps();
        managerInstance.createThreadPools();
        managerInstance.createPingThread();
        while (true) {
            Message message = aws.getOneMessageFromQueue(aws.messagesToManagerQueueName);
            if (message != null) {
                System.out.println("[DEBUG] Manager get a message from queue: " + message.body());
                String[] splittedMessage = message.body().split(aws.delimiter);
                String messageType = splittedMessage[0]; // message type is always first
                if (messageType.equals(AWS.messageType.NewTask.toString()))
                    managerInstance.handleNewTaskMessage(message);
                else if (messageType.equals(AWS.messageType.FinishedTask.toString()))
                    managerInstance.handleFinishedTaskMessage(message);
                else if (messageType.equals(AWS.messageType.Terminate.toString())) {
                    managerInstance.managerShouldTerminate.set(true);
                    managerInstance.terminateIfTasksAreDone();
                }
                aws.deleteMessageFromQueue(aws.messagesToManagerQueueName, message);
            }
        }

    }

    private void initializeQueuesAndMaps() {
        localApplicationTasksQueue = new ConcurrentLinkedQueue<Message>();
        workersTasksQueue = new ConcurrentLinkedQueue<Message>();
        localApplicationsTasksTracker = new ConcurrentHashMap<String, LocalApplicationTaskInfo>();

        // create queue manager => workers
        System.out.println("[DEBUG] Create queue in Manager.");
        aws.createQueue(aws.managerToWorkersQueueName);
    }

    private void createThreadPools() {
        System.out.println("[DEBUG] Thread pools created");
        ExecutorService localTaskHandler = Executors.newFixedThreadPool(NUMBER_OF_LOCAL_APPLICATION_THREADS);
        for (int i = 0; i < NUMBER_OF_LOCAL_APPLICATION_THREADS; i++)
            localTaskHandler.execute(new LocalApplicationTaskHandler(managerInstance));

        ExecutorService workerTaskThreadPool = Executors.newFixedThreadPool(NUMBER_OF_WORKERS_THREADS);
        for (int i = 0; i < NUMBER_OF_WORKERS_THREADS; i++)
            workerTaskThreadPool.execute(new WorkersHandler(managerInstance));
    }

    /**
     * Following function responsible to send ping message every 10 seconds to all
     * local application, letting them know the manager is still alive
     */
    private void createPingThread() {
        Thread pingThread = new Thread(() -> {
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    if (!managerShouldTerminate.get()) {
                        Iterator<String> iterator = localApplicationsTasksTracker.keySet().iterator();
                        while (iterator.hasNext()) {
                            // localApplicationId + delimiter + jobIndex
                            String localApplicationId = iterator.next().split(aws.delimiter)[0];
                            String queueName = aws.managerToLocalApplicationQueueName + localApplicationId;
                            aws.sendMessageToQueue(queueName, AWS.messageType.Ping.toString());
                        }
                    } else
                        timer.cancel();
                }
            }, 0, aws.secondsBetweenPing * 1000); // send ping message every secondsBetweenPing seconds
        });
        pingThread.start();
    }

    /*
     * Go over the list of un-finished tasks, and sum the number of required
     * workers.
     * Create or remove workers if needed
     */
    public void updateNumberOfWorkers() {
        int neededWorkers = 0;
        synchronized (localApplicationsTasksTracker) {
            for (LocalApplicationTaskInfo taskInfo : localApplicationsTasksTracker.values()) {
                neededWorkers += taskInfo.getNumberOfWorkers();
            }
            // Don't create more than MAX_WORKERS_INSTANCES instances
            neededWorkers = Math.min(neededWorkers, MAX_WORKERS_INSTANCES);
            synchronized (numberOfActivateWorker) {
                int workersDelta = neededWorkers - numberOfActivateWorker.get();
                if (workersDelta > 0) {
                    // more workers are needed
                    System.out.println("[DEBUG] create " + workersDelta + " workers");
                    createNewWorkers(workersDelta);
                } else if (workersDelta < 0) {
                    // less workers are needed
                    System.out.println("[DEBUG] create " + workersDelta + " workers");
                    removeWorkers(workersDelta, numberOfActivateWorker.get());
                }
            }
        }
    }

    private void createNewWorkers(int numberOfWorkersToCreate) {
        String workerScript = "#!/bin/bash\n" +
                "echo Worker script is running\n" +
                "echo s3://" + aws.bucketName + "/" + aws.workerJarKey + "\n" +
                "mkdir workers_files\n" +
                "aws s3 cp s3://" + aws.bucketName + "/" + aws.workerJarKey + " ./workers_files/"
                + aws.workerJarName + "\n" +
                "echo Run worker.jar\n" +
                "java -jar /workers_files/" + aws.workerJarName + "\n";
        System.out.println("[DEBUG] Create " + (numberOfWorkersToCreate) + " new workers.");
        try {
            for (int i = 0; i < numberOfWorkersToCreate; i++) {
                aws.createInstance(workerScript, aws.workerTag);
                numberOfActivateWorker.addAndGet(1);
            }
        } catch (Exception e) {
            System.out.println("[ERROR] Failed to create workers " + e.getMessage());
        }
    }

    private void removeWorkers(int numberOfWorkersToRemove, int numOfActivateWorker) {
        System.out.println("[DEBUG] Removing " +
                numberOfWorkersToRemove + " workers. Remaining workers: "
                + (numOfActivateWorker - numberOfWorkersToRemove));
        for (int i = 0; i < numberOfWorkersToRemove; i++)
            aws.sendMessageToQueue(aws.managerToWorkersQueueName,
                    createTerminateWorkerMessage());
        numberOfActivateWorker.addAndGet(numberOfWorkersToRemove * -1);
    }

    private String createTerminateWorkerMessage() {
        return AWS.messageType.Terminate.toString();
    }

    // ====== Handle add or remove of new task from local applications ======
    private void handleNewTaskMessage(Message message) {
        synchronized (localApplicationTasksLock) {
            if (!managerShouldTerminate.get()) {
                localApplicationTasksQueue.add(message);
                System.out.println(
                        "[DEBUG] Manager add a message to localApplicationTasksQueue; waking up localApplicationTaskHandler");
                localApplicationTasksLock.notify();
            } else {
                System.out.println("[DEBUG] Manager refused to get new message since he supposed to be terminated");
            }
        }
    }

    public Message getLocalApplicationTaskMessage() {
        synchronized (localApplicationTasksLock) {
            while (!localApplicationTasksQueue.isEmpty())
                return localApplicationTasksQueue.poll();
            System.out.println("[DEBUG] localApplicationTasksQueue is empty - thread goes to sleep");
            try {
                localApplicationTasksLock.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    // ===== Handle add or remove of finished task from workers =====
    private void handleFinishedTaskMessage(Message message) {
        synchronized (workersTasksLock) {
            workersTasksQueue.add(message);
            System.out.println(
                    "[DEBUG] Manager add a message to workersTasksQueue; waking up WorkersHandler");
            workersTasksLock.notify();
        }
    }

    /* Get message from workers */
    public Message getWorkersTaskMessage() {
        synchronized (workersTasksLock) {
            while (!workersTasksQueue.isEmpty())
                return workersTasksQueue.poll();
            System.out.println("[DEBUG] workersTasksQueue is empty - thread goes to sleep");
            try {
                workersTasksLock.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public ConcurrentHashMap<String, LocalApplicationTaskInfo> getLocalApplicationsTasksTracker() {
        return localApplicationsTasksTracker;
    }

    public int getNumberOfRequiredWorkers() {
        return numberOfRequiredWorkers.get();
    }

    public int AddNumberOfRequiredWorkers(int number) {
        return numberOfRequiredWorkers.addAndGet(number);
    }

    public int decreaseNumberOfRequiredWorkers(int number) {
        return numberOfRequiredWorkers.addAndGet(number * -1);
    }

    public void terminateIfTasksAreDone() {
        if (localApplicationsTasksTracker.isEmpty()) {
            System.out.println(
                    "[DEBUG] Terminating manager and all activate workers including SQS queues and deleting bucket from s3.");
            try {
                int numberOfActivateWorker = aws.getNumberOfInstances(aws.workerTag);
                for (int i = 0; i < numberOfActivateWorker; i++) // Send Terminate message to all activate workers
                    aws.sendMessageToQueue(aws.managerToWorkersQueueName, AWS.messageType.Terminate.toString());
                aws.deleteQueue(aws.messagesToManagerQueueName);
                Thread.sleep(1000 * 60); // Sleeping for 60 second to let all workers time to find termination message
                numberOfActivateWorker = aws.getNumberOfInstances(aws.workerTag);
                if (numberOfActivateWorker == 0) { // No more workers; delete worker's sqs and terminate
                    System.out.println("[DEBUG] Deleting Worker workers sqs queue");
                    aws.deleteQueue(aws.managerToWorkersQueueName);
                }
                aws.deleteBucket(aws.bucketName);
                aws.shutdownInstance(); // Terminates the manager
            } catch (Exception | Error e) {
                System.out.println("[ERROR] " + e.getMessage());
            }
        }
    }
}
