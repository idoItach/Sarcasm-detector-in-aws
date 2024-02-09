# Sarcasm detector using AWS services

# Introduction

The goal of this project is to get as an input links to amazon reviews, and by using some algorithms decide if the review is written in sarcastic way or not.
This project is combined from three different units: local application, manager and workers.
the local application is the one who runs on your local computer, while the manager and workers are running on EC2 computers of amazon.

# How to run in by yourself

1. clone the repository to your own computer.
2. create JAR files for the manager and the worker by using 'mvn clean package' command in the root directory of the manager and the worker.
3. rename the JAR files to 'manager.jar' and 'worker.jar', and put them in some location
4. inside the local application/src/main/java/localApplication.java set MANAGER_JAR_PATH and WORKER_JAR_PATH to the path of the JARs.
5. put the required inputs to run in some folder, and copy its path to LOCAL_DICTIONARY_PATH variable in same file.
6. set your AWS credentials in ~user/.aws/credentials files
7. run local application with the required arguments:
   inputFileName1.txt ... inputFileNameN.txt outputFileName1.html ... outputFileNameN.html n terminate
   where for each input file you need to decide on the output file's name
   n represent the number of reviews per each worker (more on that below). note that there is a constant MAX_WORKERS_INSTANCES in manager main file that we cannot create more instances of workers than this number (due to AWS restricts on students account)
   terminate is a boolean (so write 'true' or 'false') that indicates if the manager should terminate himself after finish processing our inputs.
8. wait till output files created, and see the results

# How it works?

- Upon running the local application, the manager and worker JARs are uploaded to Amazon's S3 storage service. Subsequently, the input files are also uploaded to S3, and SQS queues are created for communication between the manager and the local application. We are creating at this stage 2 SQS queues; one queue for sending message to the manager (which will be used also by the workers) - messagesToManagerQueueName, and a queue for sending messages from the manager to local application - managerToLocalApplicationQueueName. Following this setup, the system checks for an active instance of the manager in EC2. If none exists, one is created. Then, a message regarding the input files awaiting processing is sent to the manager.
- The manager, creates a SQS queue that will be used to send messages to the workers (managerToWorkersQueueName), and then he is listening to the SQS message queue (messagesToManagerQueueName). When he receives a new message from the local application containing the link to the input file in S3. It downloads the file, splits it into separate reviews, and creates a task in the workers SQS for each review. The manager determines the required number of workers to run in the cloud based on the input parameter 'n' provided during the execution of the local application and the number of reviews. It can dynamically scale by creating additional instances if more computing power is needed to complete the tasks.
- Each worker retrieves a message from the workers SQS, processes it, sends the output to the manager SQS, and waits for the next task message. This process continues until the worker receives a termination message from the manager.
- Upon receiving results for each review of an input, the manager uploads the outputs to S3 and sends the local application requesting the service a link to the output text file. The manager updates the required computing power after completing the processing of an input file and determines whether to terminate one or more workers based on the workload from all local applications using the service.
- The local application retrieves the message from SQS, downloads the input file, and generates a simple HTML file to display the output.

# Persistence

Given that instances aren't always reliable, we've implemented a couple of strategies to ensure smooth operation:

- The manager sends a ping message to the local application every 10 seconds to signal that it's still active. If the local application doesn't receive a ping message for longer than the time defined in TIME_BEFORE_RECREATE_MANAGER, it triggers a manager recreation process. This guarantees that if something goes wrong, we recreate the manager and resend the message for processing.
- When a worker claims a message for processing, we ensure it won't get lost. If, for instance, a worker shuts down unexpectedly after claiming a message, SQS has a built-in fail-safe mechanism (Amazon SQS visibility timeout). This mechanism ensures that if a worker receives a message but fails to delete it (we only delete it after sending the output to the manager), the message is returned to the queue. This way, we can be sure that all task messages will be handled by a worker.

# Scalability

The project has been designed to efficiently handle large volumes of clients and data. The manager is implemented using the reactor pattern, enabling it to seamlessly manage numerous requests from local applications for processing input files. Java's sleep and notify mechanisms are leveraged to optimize resource utilization without resorting to busy-waiting.
Moreover, the manager dynamically creates worker instances as required and terminates them when they are no longer needed. This allows us to handle plenty of tasks while using cloud services effectively.

# Security

There are a lot of people who would love to have an access to our AWS account and use the computing resources he needs.
By keeping our credentials separate and inaccessible within the project files, we safeguard against potential misuse of our computing resources by unauthorized individuals.

# Use of EC2

We used an exist amazon's AMI: ami-00e95a9222311e8ed, and use T2_LARGE instance type for the manager and workers.
