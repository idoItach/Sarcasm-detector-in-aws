import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Date;
import java.util.List;

import software.amazon.awssdk.services.sqs.model.Message;

public class localApplication {
    final static AWS aws = AWS.getInstance();
    public static final String MANAGER_JAR_PATH = "D:\\assignment1jars\\manager.jar";
    public static final String WORKER_JAR_PATH = "D:\\assignment1jars\\worker.jar";
    public static final String LOCAL_DICTIONARY_PATH = "D:\\לימודים\\סמסטר ז\\תכנות מערכות מבוזרות\\Assignment 1\\localApplication\\";

    static String[] inFilePaths;
    static String[] outFilePaths;
    static int tasksPerWorker;
    static boolean shouldTerminateAfterProcessing;
    static String localApplicationId;
    static String localManagerToLocalApplicationQueueName;
    static String managerInstanceID;
    static int finishedTasks = 0;
    static long lastPing;
    static long TIME_BEFORE_RECREATE_MANAGER = 120 * 1000; // wait 120 seconds without ping before recreate manager

    public static void main(String[] args) {// args = [inFilePath, outFilePath, tasksPerWorker, terminateMessage]
        getAndValidateArguments(args);
        initializeLocalApp();
        runLocalApplicationLogic();
    }

    private static void runLocalApplicationLogic() {
        // set infrastructure
        uploadJarsToS3();
        createQueues();
        createManager();

        // work on local application inputs
        uploadInputsToS3();
        lastPing = getCurrentTime();
        // wait for results of all inputs
        waitForAllResults();
        if (shouldTerminateAfterProcessing) {
            sendTerminateMessage();
        }
        System.out.println("[INFO] " + localApplicationId + " is finished.");
    }

    private static void getAndValidateArguments(String[] args) {
        if (args.length < 4 || args.length % 2 != 0) {
            // at least 4 arguments should pass, and it must be even
            System.out.println("[ERROR] mismatch with arguments");
            System.exit(1);
        } else {
            final int numOfFiles = (args.length - 2) / 2; // num of args - 2 for terminate and workers
            inFilePaths = new String[numOfFiles];
            for (int i = 0; i < numOfFiles; i++) {
                inFilePaths[i] = args[i];
                // assert input file path is from .txt format
                if (!inFilePaths[i].substring(inFilePaths[i].indexOf('.') + 1).equals("txt")) {
                    System.out.println("[ERROR] " + inFilePaths[i] + " is not txt file");
                    System.exit(1);
                }
            }

            outFilePaths = new String[numOfFiles];
            for (int i = 0; i < numOfFiles; i++) {
                outFilePaths[i] = args[i + numOfFiles];
                // assert output file path is from .html format
                if (!outFilePaths[i].substring(outFilePaths[i].indexOf('.') + 1).equals("html")) {
                    System.out.println("[ERROR] " + outFilePaths[i] + " is not html file");
                    System.exit(1);
                }
            }

            tasksPerWorker = Integer.valueOf(args[args.length - 2]);

            if ("true".equals(args[args.length - 1])) {
                shouldTerminateAfterProcessing = true;
            } else {
                shouldTerminateAfterProcessing = false;
            }
        }
    }

    private static void initializeLocalApp() {
        localApplicationId = "localApp" + getCurrentTime();
        localManagerToLocalApplicationQueueName = aws.managerToLocalApplicationQueueName + localApplicationId;
    }

    private static void uploadJarsToS3() {
        System.out.println("[DEBUG] Create bucket in S3");
        aws.createBucketIfNotExists(aws.bucketName);
        System.out.println("[DEBUG] Upload Manager and Worker jars to S3");
        if (!aws.checkIfFileExistsInS3(aws.bucketName, aws.managerJarKey)) {
            aws.uploadFileToS3(aws.bucketName, aws.managerJarKey, new File(MANAGER_JAR_PATH));
            System.out.println("[DEBUG] uploaded Manager Jar to S3");
        }
        if (!aws.checkIfFileExistsInS3(aws.bucketName, aws.workerJarKey)) {
            aws.uploadFileToS3(aws.bucketName, aws.workerJarKey, new File(WORKER_JAR_PATH));
            System.out.println("[DEBUG] uploaded Worker Jar to S3");
        }
        System.out.println("[DEBUG] Finish upload Jars to S3.");
    }

    private static void createQueues() {
        // create one queue to send messages to manager, and one queue for each
        // localApplication
        System.out.println("[DEBUG] Create queues in local application.");
        aws.createQueue(aws.messagesToManagerQueueName);
        aws.createQueue(localManagerToLocalApplicationQueueName);
    }

    /** create manager only if not exist */
    private static void createManager() {
        System.out.println("[DEBUG] Check if manager exist.");
        if (!aws.isManagerActive()) {
            System.out.println("[DEBUG] Manager doesn't exist - creating one.");
            String managerScript = "#!/bin/bash\n" +
                    "echo Manager script is running\n" +
                    "echo s3://" + aws.bucketName + "/" + aws.managerJarKey + "\n" +
                    "mkdir manager_files\n" +
                    "aws s3 cp s3://" + aws.bucketName + "/" + aws.managerJarKey + " ./manager_files/"
                    + aws.managerJarName + "\n" +
                    "echo Run manager.jar\n" +
                    "java -jar /manager_files/" + aws.managerJarName + "\n";

            managerInstanceID = aws.createInstance(managerScript, aws.managerTag);
            System.out.println("[DEBUG] Manager created and started!.");
        } else {
            System.out.println("[DEBUG] Manager already exist.");
        }
    }

    private static void uploadInputsToS3() {
        System.out.println("[DEBUG] Upload inputs to S3.");
        for (int index = 0; index < inFilePaths.length; index++) {
            System.out.println("[DEBUG] Upload file with index " + index + " and name: " + inFilePaths[index]);
            String taskKey = localApplicationId + aws.delimiter + index;
            aws.uploadFileToS3(aws.bucketName, taskKey, new File(LOCAL_DICTIONARY_PATH + inFilePaths[index]));
            System.out.println(
                    "[DEBUG] Finished uploading file with name " + inFilePaths[index] + ", creating task message");
            String taskMessage = createNewTaskMessage(localApplicationId, index);
            aws.sendMessageToQueue(aws.messagesToManagerQueueName, taskMessage);
        }
    }

    private static String createNewTaskMessage(String localApplicationId, int index) {
        System.out.println("[DEBUG] createNewTaskMessage function");
        return AWS.messageType.NewTask.toString() +
                aws.delimiter +
                aws.bucketName +
                aws.delimiter +
                localApplicationId +
                aws.delimiter + index +
                aws.delimiter +
                tasksPerWorker;
    }

    private static void waitForAllResults() {
        System.out.println("[DEBUG] WaitingForAllResults");
        while (true) {
            List<Message> messages = aws
                    .getAllMessagesFromQueue(localManagerToLocalApplicationQueueName);
            System.out.println("[DEBUG] Found " + messages.size() + " messages.");
            if (messages.size() > 0) {
                for (Message message : messages) {
                    // message: messageType, localApplicationId, inputIndex, s3key
                    String[] messageParts = message.body().split(aws.delimiter);
                    if (messageParts[0].equals(AWS.messageType.Ping.toString())) {
                        System.out.println("[DEBUG] Ping message from server received");
                        lastPing = getCurrentTime();
                        aws.deleteMessageFromQueue(localManagerToLocalApplicationQueueName, message);
                    }
                    // Check if the message related to this localApplication
                    else if (messageParts[1].startsWith(localApplicationId)) {
                        if (messageParts[0].equals(AWS.messageType.FinishedTask.toString())) {
                            // messageType, localApplicationId, inputIndex, s3key(with taskDelimiter)
                            String s3key = messageParts[3] + aws.delimiter + messageParts[4];
                            String outputAsString = aws.getObjectFromS3(aws.bucketName, s3key);
                            saveOutputStringToFile(outputAsString, Integer.parseInt(messageParts[2]));
                            aws.deleteMessageFromQueue(localManagerToLocalApplicationQueueName, message);
                            finishedTasks++;
                            if (finishedTasks == outFilePaths.length) {
                                aws.deleteQueue(localManagerToLocalApplicationQueueName);
                                return;
                            }
                        }
                    }
                }
                // no message received
            } else if (getCurrentTime() - lastPing > TIME_BEFORE_RECREATE_MANAGER) {
                System.out.println("[DEBUG] Time for recreate manager");
                recreateManager();
                break;
            }
        }
    }

    private static void saveOutputStringToFile(String outputAsString, int inputIndex) {
        String htmlOpenTag = "<!DOCTYPE html><html><body><table border=\"2\"><tr><th>Link to review</th><th>Entities</th><th>Is sarcasm</th></tr>";
        String htmlCloseTag = "</table></body></html>";
        String[] outputLines = outputAsString.split("\n");
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(outFilePaths[inputIndex]));
            writer.write(htmlOpenTag);
            for (String line : outputLines) {
                // reviewOutput: link, sentiment, entities, isSarcasm
                String[] reviewOutput = line.split(aws.taskDelimiter);
                writer.write("<tr><th style='color:" + getLinkColor(reviewOutput[1]) + "'>" + reviewOutput[0]
                        + "</th><th>" + reviewOutput[2]
                        + "</th><th>" + reviewOutput[3] + "</th></tr>");
            }
            writer.write(htmlCloseTag);
            writer.close();
            System.out.println("[DEBUG] Output has been created");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getLinkColor(String sentiment) {
        if ("0".equals(sentiment))
            return "#8B0000"; // dark red
        if ("1".equals(sentiment))
            return "#ff0000"; // red
        if ("2".equals(sentiment))
            return "#000000"; // black
        if ("3".equals(sentiment))
            return "#90EE90"; // light green
        if ("4".equals(sentiment))
            return "#023020"; // dark green
        else
            return "";

    }

    private static void sendTerminateMessage() {
        System.out.println("[DEBUG] " + localApplicationId + " send terminate message to manger");
        aws.sendMessageToQueue(aws.messagesToManagerQueueName, AWS.messageType.Terminate.toString());
    }

    private static void recreateManager() {
        aws.terminateInstance(managerInstanceID);
        runLocalApplicationLogic();
    }

    private static long getCurrentTime() {
        return new Date().getTime();
    }
}