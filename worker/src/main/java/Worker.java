import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import software.amazon.awssdk.services.sqs.model.Message;

public class Worker {
    final static AWS aws = AWS.getInstance();
    static sentimentAnalysisHandler sentimentAnalysisHandler = new sentimentAnalysisHandler();
    static namedEntityRecognitionHandler namedEntityRecognitionHandler = new namedEntityRecognitionHandler();

    public static void main(String[] args) {
        System.out.println("[DEBUG] Worker stated");
        while (true) {
            Message message = aws.getOneMessageFromQueue(aws.managerToWorkersQueueName);
            if (message != null) {
                AtomicBoolean isFinishedProcessingMessage = new AtomicBoolean(false);
                extendMessageVisibility(message, isFinishedProcessingMessage);
                // message: messageType, localApplicationId, inputIndex, review, reviewIndex
                String[] splittedMessage = message.body().split(aws.delimiter);
                String messageType = splittedMessage[0];
                if (messageType.equals(AWS.messageType.NewTask.toString())) {
                    System.out.println("[DEBUG] Worker get newTask message");
                    String localApplicationId = splittedMessage[1];
                    String inputIndex = splittedMessage[2];
                    String review = splittedMessage[3];
                    String reviewIndex = splittedMessage[4];
                    // review: link, text, rating
                    String[] splittedReview = review.split(aws.taskDelimiter);
                    String link = splittedReview[0];
                    String reviewText = splittedReview[1];
                    int rating = Integer.parseInt(splittedReview[2]);
                    // processedReview: sentiment, entities, isSarcasm
                    String[] processedReview = processReview(reviewText, rating);
                    String finishedTaskMessage = createFinishedTaskMessage(link, localApplicationId, inputIndex,
                            processedReview, reviewIndex);
                    // After processing review, submit results to manager
                    aws.sendMessageToQueue(aws.messagesToManagerQueueName, finishedTaskMessage);
                    isFinishedProcessingMessage.set(true);
                    aws.deleteMessageFromQueue(aws.managerToWorkersQueueName, message);
                } else if (messageType.equals(AWS.messageType.Terminate.toString())) {
                    System.out.println("[DEBUG] Worker get terminate message; shutdown the instance");
                    aws.deleteMessageFromQueue(aws.managerToWorkersQueueName, message);
                    aws.shutdownInstance();
                    break;
                } else {
                    System.out.println("[DEBUG] Worker get un-handle message from type: " + messageType);
                }
            }

        }
    }

    private static String createFinishedTaskMessage(String link, String localApplicationId, String inputIndex,
            String[] processedReview, String reviewIndex) {
        // message: messageType, localApplicationId, inputIndex, reviewIndex, link,
        // sentiment, entities, isSarcasm
        String finishedTaskMessage = AWS.messageType.FinishedTask.toString() + aws.delimiter + localApplicationId
                + aws.delimiter
                + inputIndex + aws.delimiter + reviewIndex
                + aws.delimiter + link + aws.delimiter +
                processedReview[0] + aws.delimiter + processedReview[1] + aws.delimiter + processedReview[2];
        System.out.println("[DEBUG] finished task message: " + finishedTaskMessage);
        return finishedTaskMessage;
    }

    private static String[] processReview(String reviewText, int rating) {
        // 0 - very negative, 4 - very positive
        int sentiment = sentimentAnalysisHandler.findSentiment(reviewText);
        List<String> entities = namedEntityRecognitionHandler.extractEntities(reviewText);
        // in case there is a gap of at least 3 between rating and sentiment, consider
        // as sarcasm review
        boolean isSarcasm = Math.abs(rating - sentiment) >= 3;

        // Convert the values to strings and return as an array
        return new String[] { String.valueOf(sentiment), String.join(",", entities), String.valueOf(isSarcasm) };
    }

    private static void extendMessageVisibility(Message message, AtomicBoolean isFinishedProcessingMessage) {
        String receipt = message.receiptHandle();
        Thread timerThread = new Thread(() -> {
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    if (!isFinishedProcessingMessage.get()) // if not finish processing - increase message visibility
                        aws.changeMessageVisibility(aws.managerToWorkersQueueName, receipt);
                    else {
                        timer.cancel();
                    }
                }
            }, 0, 60 * 1000); // every 60 second the run mission is executed - increase message visibility
        });
        timerThread.start();
    }
}
