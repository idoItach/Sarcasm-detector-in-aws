import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalApplicationTaskInfo {
    private Map<String, String> taskResults;
    private AtomicInteger numberOfTasksLeft;
    private final int numberOfWorkers;

    public LocalApplicationTaskInfo(int numberOfTasksLeft, int numberOfWorkers) {
        this.taskResults = new ConcurrentHashMap<>();
        this.numberOfTasksLeft = new AtomicInteger(numberOfTasksLeft);
        this.numberOfWorkers = numberOfWorkers;
    }

    public Map<String, String> getTaskResults() {
        return taskResults;
    }

    public void setTaskResult(String taskKey, String output) {
        this.taskResults.putIfAbsent(taskKey, output);
    }

    public int getNumberOfTasksLeft() {
        return numberOfTasksLeft.get();
    }

    public int decrementNumberOfReviewsLeft() {
        return this.numberOfTasksLeft.decrementAndGet();
    }

    public int getNumberOfWorkers() {
        return numberOfWorkers;
    }

}