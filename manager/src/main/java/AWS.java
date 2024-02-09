import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

public class AWS {
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;

    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    public final String managerJarKey = "ManagerJar";
    public final String workerJarKey = "WorkerJar";

    public final String managerJarName = "manager.jar";
    public final String workerJarName = "worker.jar";

    public final String managerTag = "Manager";
    public final String workerTag = "Worker";

    public final String AMI = "ami-00e95a9222311e8ed";

    public final String bucketName = "mevuzrot-is-the-best-course-in-bgu";

    public final String managerToWorkersQueueName = "managerToWorkersSQS";
    public final String messagesToManagerQueueName = "messagesToManagerSQS";
    public final String managerToLocalApplicationQueueName = "managerToLocalApplicationSQS";

    public enum messageType {
        NewTask,
        FinishedTask,
        Ping,
        Terminate;
    }

    public final String delimiter = "@@";
    public final String taskDelimiter = "##";

    public final int secondsBetweenPing = 10;

    private static final AWS instance = new AWS();

    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region2).build();
    }

    public static AWS getInstance() {
        return instance;
    }

    // Ec2
    // check if a manager node is active
    public boolean isManagerActive() {
        String nextToken = null;
        try {
            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);
                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        if (instance.state().name().equals(InstanceStateName.RUNNING) ||
                                instance.state().name().equals(InstanceStateName.PENDING)) { // TODO check
                            for (Tag tag : instance.tags()) {
                                if (tag.key().equals("Name") && tag.value().equals("Manager")) {
                                    ec2.close();
                                    return true;
                                }
                            }
                        }
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] in checking if manager is active: " + e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        // sqs.close();
        // s3.close();
        // ec2.close();

        return false;
    }

    // Ec2
    public String createInstance(String userData, String tagName) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_LARGE) // TODO: check if need other type
                .imageId(AMI)
                .maxCount(1)
                .minCount(1)
                .keyName("vockey") // TODO: check
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .instanceInitiatedShutdownBehavior("terminate") // TODO: check if needed
                .userData(Base64.getEncoder().encodeToString(userData.getBytes()))
                .build();
        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("Name")
                .value(tagName)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.println("[DEBUG] Tag added successfully to instance: " + instanceId + "with ami: " + AMI
                    + "and tag: " + tagName);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] in creating instance: " + e.getMessage());
            System.exit(1);
        }

        return instanceId;
    }

    // Ec2
    public void terminateInstance(String instanceId) {
        try {
            TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            ec2.terminateInstances(terminateRequest);

            System.out.println("[DEBUG] Instance terminated successfully: " + instanceId);
        } catch (Ec2Exception e) {
            System.err.println("[ERROR] in terminating instance: " + e.awsErrorDetails().errorMessage());
        }
    }

    // Ec2
    public void shutdownInstance() {
        TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                .instanceIds(EC2MetadataUtils.getInstanceId())
                .build();
        ec2.terminateInstances(terminateRequest);
    }

    // EC2
    public int getNumberOfInstances(String tagName) {
        String nextToken = null;
        int numberOfInstances = 0;
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest
                    .builder().nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    if (instance.state().name().toString().equals("running")) {
                        for (software.amazon.awssdk.services.ec2.model.Tag tag : instance.tags()) {
                            if (tag.key().equals("Name") && tag.value().equals(tagName)) {
                                numberOfInstances++;
                            }
                        }
                    }
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);
        return numberOfInstances;
    }

    // S3
    // this method attempts to create an S3 bucket with the specified name and
    // region.
    // If the bucket already exists, the method will not throw an exception, and if
    // it doesn't exist,
    // it will wait until the bucket is created (polling) before returning.
    public void createBucketIfNotExists(String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
            System.out.println("[ERROR] in creating bucket in S3: " + e.getMessage());
        }
    }

    // s3
    public void uploadFileToS3(String bucketName, String key, File file) {
        PutObjectRequest req = PutObjectRequest.builder().bucket(bucketName).key(key).build();
        s3.putObject(req, RequestBody.fromFile(file));
    }

    // s3
    // Get the content of an object from an S3 bucket as a string
    public String getObjectFromS3(String bucketName, String key) {
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(getObjectRequest);
            byte[] contentBytes = objectBytes.asByteArray();
            return new String(contentBytes, StandardCharsets.UTF_8);
        } catch (S3Exception e) {
            System.err.println("[ERROR] in getting object from bucket: " + e.awsErrorDetails().errorMessage());
            return null;
        }
    }

    // s3
    // TODO: maybe a function that returns a list of all the objects in a bucket

    // s3
    // Delete one object from an S3 bucket
    public void deleteObjectFromBucket(String bucketName, String key) {
        try {
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            s3.deleteObject(deleteObjectRequest);
            System.out.println("[DEBUG] Object deleted successfully from: " + bucketName + " key:" + key);
        } catch (S3Exception e) {
            System.err.println("[ERROR] in deleting object from bucket: " + e.awsErrorDetails().errorMessage());
        }
    }

    // s3
    // Delete all objects from an S3 bucket
    public void deleteAllObjectsFromBucket(String bucketName) {
        try {
            // To delete a bucket, all the objects in the bucket must be deleted first.
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .build();
            ListObjectsV2Response listObjectsV2Response;

            // S3 buckets can contain a large number of objects, and the result of listing
            // objects might be paginated
            // to improve performance and reduce the amount of data transferred in a single
            // response
            do {
                listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
                for (S3Object s3Object : listObjectsV2Response.contents()) {
                    DeleteObjectRequest request = DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(s3Object.key())
                            .build();
                    s3.deleteObject(request);
                }
            } while (listObjectsV2Response.isTruncated());
            System.out.println("[DEBUG] All objects deleted from the bucket: " + bucketName);
        } catch (S3Exception e) {
            System.err.println("[ERROR] in deleting objects: " + e.awsErrorDetails().errorMessage());
        }
    }

    // s3
    // the bucket must be empty before it can be deleted
    public void deleteBucket(String bucketName) {
        try {
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            s3.deleteBucket(deleteBucketRequest);
            System.out.println("[DEBUG] Bucket deleted successfully: " + bucketName);
        } catch (S3Exception e) {
            System.err.println("[ERROR] in deleting bucket: " + e.awsErrorDetails().errorMessage());
        }
    }

    // sqs
    public void createQueue(String queueName) {
        try {
            Map<QueueAttributeName, String> map = new HashMap<>();
            map.put(QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS, "20");
            // it sets the RECEIVE_MESSAGE_WAIT_TIME_SECONDS attribute to "20".
            // This attribute represents the time for which a ReceiveMessage call will wait
            // for a message to arrive in the queue before returning an empty response if no
            // messages are available.
            // By setting it to 20 seconds, it means that if the queue is empty,
            // the ReceiveMessage operation will wait for up to 20 seconds for a message to
            // arrive before returning an empty response.
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .attributes(map)
                    .build();
            sqs.createQueue(request);
            System.out.println("[DEBUG] Queue created successfully: " + queueName);
        } catch (SqsException e) {
            System.err.println("[ERROR] creating queue: " + e.awsErrorDetails().errorMessage());
        }

    }

    // sqs
    public void sendMessageToQueue(String queueName, String message) {
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(message)
                    .build();
            sqs.sendMessage(sendMsgRequest);
        } catch (SqsException e) {
            System.err.println("[ERROR] sending message to SQS: " + e.awsErrorDetails().errorMessage());
        }
    }

    // sqs
    public List<Message> getAllMessagesFromQueue(String queueName) {
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            return messages;
        } catch (SqsException e) {
            System.err.println("[ERROR] retrieving all messages from SQS: " + e.awsErrorDetails().errorMessage());
            return null;
        }
    }

    // sqs
    public Message getOneMessageFromQueue(String queueName) {
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            return messages.size() > 0 ? messages.get(0) : null;
        } catch (SqsException e) {
            System.err.println("[ERROR] retrieving one message from SQS: " + e.awsErrorDetails().errorMessage());
            return null;
        }
    }

    // sqs
    public void deleteMessageFromQueue(String queueName, Message message) {
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            sqs.deleteMessage(deleteMessageRequest);
        } catch (SqsException e) {
            System.err.println("[ERROR] deleting message from SQS: " + e.awsErrorDetails().errorMessage());
        }
    }

    // sqs
    public void deleteQueue(String queueName) {
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();
            sqs.deleteQueue(deleteQueueRequest);
            System.out.println("[DEBUG] Queue deleted successfully: " + queueName);
        } catch (SqsException e) {
            System.err.println("[ERROR] deleting queue: " + e.awsErrorDetails().errorMessage());
        }
    }

    public void changeMessageVisibility(String queueName, String receipt) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

        sqs.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                .queueUrl(queueUrl)
                .visibilityTimeout(60)
                .receiptHandle(receipt)
                .build());
    }
}