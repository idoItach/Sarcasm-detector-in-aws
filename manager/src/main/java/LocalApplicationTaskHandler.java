/* LocalApplicationTaskHandler receive work orders from local application, and should:
 * 1. Separate them to small tasks
 * 2. Inform the manager that new task is coming, and maybe he should create more workers
*/

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.sqs.model.Message;

public class LocalApplicationTaskHandler extends Thread {
    final AWS aws = AWS.getInstance();
    private Manager manager;

    public LocalApplicationTaskHandler(Manager manager) {
        System.out.println("[DEBUG] LocalApplicationTaskHandler created");
        this.manager = manager;
    }

    @Override
    public void run() {
        while (true) {
            Message message = manager.getLocalApplicationTaskMessage(); // in case of empty queue - return null;
            if (message != null) {
                System.out.println("[DEBUG] localApplicationTaskHandler gets a message: " + message.body());
                String[] splittedMessage = message.body().split(aws.delimiter);
                // message: messageType, bucketName, localApplicationId, inputIndex,
                // tasksPerWorker
                try {
                    handleNewTaskMessage(splittedMessage[1], splittedMessage[2], splittedMessage[3],
                            Integer.parseInt(splittedMessage[4]));
                } catch (Exception e) {
                    // in case we failed to handle the message, we will retry handle it by insert it
                    // again to the sqs queue.
                    if (message != null) {
                        System.out.println("[DEBUG] Manager failed to handle new task message; return it to queue");
                        aws.sendMessageToQueue(aws.messagesToManagerQueueName, message.body());
                    }
                }
            }
        }
    }

    private void handleNewTaskMessage(String bucketName, String localApplicationId, String inputIndex,
            int tasksPerWorker) {
        // in in bucket, the taskKey is combined from localApplicationId+inputIndex (it
        // is the key for the input to process)
        String keyInBucket = localApplicationId + aws.delimiter + inputIndex;
        String reviewsFile = aws.getObjectFromS3(bucketName, keyInBucket);
        List<String> reviews = extractReviews(reviewsFile);
        int numberOfReviews = reviews.size();
        int numberOfWorkers = (int) Math.floor(numberOfReviews / tasksPerWorker);
        LocalApplicationTaskInfo taskInfo = new LocalApplicationTaskInfo(numberOfReviews, numberOfWorkers);
        manager.getLocalApplicationsTasksTracker().put(keyInBucket, taskInfo);
        manager.AddNumberOfRequiredWorkers(numberOfWorkers);
        manager.updateNumberOfWorkers(); // create more workers if needed and we can
        createTasksForWorkers(reviews, localApplicationId, inputIndex);
    }

    private void createTasksForWorkers(List<String> reviews, String localApplicationId, String inputIndex) {
        for (int i = 0; i < reviews.size(); i++) {
            aws.sendMessageToQueue(aws.managerToWorkersQueueName,
                    createWorkerTaskMessage(reviews.get((i)), localApplicationId, inputIndex, i));
        }
    }

    private String createWorkerTaskMessage(String review, String localApplicationId, String inputIndex,
            int reviewIndex) {
        String message = AWS.messageType.NewTask.toString() + aws.delimiter + localApplicationId + aws.delimiter
                + inputIndex + aws.delimiter + review + aws.delimiter + reviewIndex;
        System.out.println("[DEBUG] manager send the following message to workers queue:" + message);
        return message;
    }

    public List<String> extractReviews(String jsonFile) {
        List<String> extractedReviewsData = new ArrayList<>();

        JsonFactory factory = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper();

        try (JsonParser parser = factory.createParser(jsonFile)) {
            while (parser.nextToken() != null) {
                JsonNode jsonNode = mapper.readTree(parser);
                if (jsonNode != null && jsonNode.has("reviews")) {
                    JsonNode reviewsArray = jsonNode.get("reviews");

                    Iterator<JsonNode> reviewsIterator = reviewsArray.elements();
                    while (reviewsIterator.hasNext()) {
                        JsonNode review = reviewsIterator.next();

                        String link = review.get("link").asText();
                        String text = review.get("text").asText();
                        int rating = review.get("rating").asInt();

                        String reviewData = link + aws.taskDelimiter + text + aws.taskDelimiter + rating;
                        extractedReviewsData.add(reviewData);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return extractedReviewsData;
    }
}
