import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Collection;

import software.amazon.awssdk.services.sqs.model.Message;

public class WorkersHandler extends Thread {

    final AWS aws = AWS.getInstance();
    private Manager manager;

    public WorkersHandler(Manager manager) {
        System.out.println("[DEBUG] WorkersHandler created");
        this.manager = manager;
    }

    @Override
    public void run() {
        while (true) {
            Message message = manager.getWorkersTaskMessage(); // in case of empty queue - return null;
            if (message != null) {
                System.out.println("[DEBUG] workersHandler gets a message: " + message.body());
                try {
                    handleWorkerTaskMessage(message);
                } catch (Exception e) {
                    // in case we failed to handle the message, we will retry handle it by insert it
                    // again to the sqs queue.
                    if (message != null) {
                        System.out
                                .println("[DEBUG] Manager failed to handle finished task message; return it to queue");
                        aws.sendMessageToQueue(aws.messagesToManagerQueueName, message.body());
                    }
                }
            }
        }
    }

    /** Handle workers messages regards finished tasks */
    private void handleWorkerTaskMessage(Message message) {
        // message: messageType, localApplicationId, inputIndex, reviewIndex, link,
        // sentiment, entities, isSarcasm
        String[] splittedMessage = message.body().split(aws.delimiter);
        String localApplicationId = splittedMessage[1];
        String inputIndex = splittedMessage[2];
        String reviewIndex = splittedMessage[3];
        String taskKey = localApplicationId + aws.delimiter + inputIndex;
        // output: link, sentiment, entities, isSarcasm, separated by taskDelimiter
        String output = splittedMessage[4] + aws.taskDelimiter + splittedMessage[5] + aws.taskDelimiter
                + splittedMessage[6] + aws.taskDelimiter + splittedMessage[7] + "\n";
        try {
            LocalApplicationTaskInfo localApplicationTaskInfo = manager.getLocalApplicationsTasksTracker().get(taskKey);
            localApplicationTaskInfo.setTaskResult(reviewIndex, output);
            int numberOfReviewsLeft = localApplicationTaskInfo.decrementNumberOfReviewsLeft();
            if (numberOfReviewsLeft == 0) {
                System.out.println("[DEBUG] Workers are finished to work on " + localApplicationId
                        + " task with input index " + inputIndex);
                finishLocalApplicationTask(localApplicationId, inputIndex, localApplicationTaskInfo);
                manager.getLocalApplicationsTasksTracker().remove(taskKey);
                manager.updateNumberOfWorkers();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void finishLocalApplicationTask(String localApplicationId, String inputIndex,
            LocalApplicationTaskInfo localApplicationTaskInfo) {
        String output = processOutputFile(localApplicationId, inputIndex, localApplicationTaskInfo);
        String outputPath = "./" + localApplicationId + inputIndex + ".txt";
        try {
            PrintWriter printWriter = new PrintWriter(outputPath);
            printWriter.print(output);
            printWriter.close();
            File file = new File(outputPath);
            String s3keyForOutput = "outputs/" + localApplicationId + aws.delimiter + inputIndex + ".txt";
            aws.uploadFileToS3(aws.bucketName, s3keyForOutput, file);
            String queueName = aws.managerToLocalApplicationQueueName + localApplicationId;
            aws.sendMessageToQueue(queueName,
                    createMessageToLocalApplication(localApplicationId, inputIndex, s3keyForOutput));
            file.delete();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private String createMessageToLocalApplication(String localApplicationId, String inputIndex,
            String s3keyForOutput) {
        // messageType, localApplicationId, inputIndex, s3key(with taskDelimiter)
        String message = AWS.messageType.FinishedTask + aws.delimiter + localApplicationId + aws.delimiter + inputIndex
                + aws.delimiter + s3keyForOutput;
        System.out.println("[DEBUG] manager send the following message to localApplication queue:" + message);
        return message;

    }

    private String processOutputFile(String localApplicationId, String inputIndex,
            LocalApplicationTaskInfo localApplicationTaskInfo) {
        StringBuilder output = new StringBuilder();
        Collection<String> workersOutputs = localApplicationTaskInfo.getTaskResults().values();
        for (String workerOutput : workersOutputs) {
            output.append(workerOutput);
        }
        return output.toString();
    }

}
