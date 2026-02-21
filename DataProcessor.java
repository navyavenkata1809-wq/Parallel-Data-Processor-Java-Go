import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Custom Exception for processing errors.
 */
class ProcessingException extends Exception {
    public ProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}

/**
 * Represents a piece of data to be processed.
 */
class Task {
    private final int id;
    private final String payload;

    public Task(int id, String payload) {
        this.id = id;
        this.payload = payload;
    }

    public int getId() { return id; }
    public String getPayload() { return payload; }

    @Override
    public String toString() {
        return "Task-" + id + " [" + payload + "]";
    }
}

/**
 * Thread-safe shared queue implementation using Locks.
 */
class TaskQueue {
    private final Queue<Task> queue = new LinkedList<>();
    private final int capacity;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();
    private boolean isShutdown = false;

    public TaskQueue(int capacity) {
        this.capacity = capacity;
    }

    public void addTask(Task task) throws InterruptedException {
        lock.lock();
        try {
            while (queue.size() == capacity && !isShutdown) {
                notFull.await();
            }
            if (isShutdown) return;
            queue.add(task);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    public Task getTask() throws InterruptedException {
        lock.lock();
        try {
            while (queue.isEmpty() && !isShutdown) {
                notEmpty.await();
            }
            if (queue.isEmpty() && isShutdown) return null;
            Task task = queue.poll();
            notFull.signal();
            return task;
        } finally {
            lock.unlock();
        }
    }

    public void shutdown() {
        lock.lock();
        try {
            isShutdown = true;
            notEmpty.signalAll();
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }
}

/**
 * Result Logger to simulate a shared output resource (file).
 */
class ResultLogger {
    private final String fileName;

    public ResultLogger(String fileName) {
        this.fileName = fileName;
    }

    public synchronized void logResult(String result) throws IOException {
        try (PrintWriter out = new PrintWriter(new FileWriter(fileName, true))) {
            out.println(result);
        }
    }
}

/**
 * Worker implementation that processes tasks.
 */
class Worker implements Runnable {
    private final int workerId;
    private final TaskQueue taskQueue;
    private final ResultLogger logger;
    private final Random random = new Random();

    public Worker(int workerId, TaskQueue taskQueue, ResultLogger logger) {
        this.workerId = workerId;
        this.taskQueue = taskQueue;
        this.logger = logger;
    }

    @Override
    public void run() {
        System.out.println("Worker-" + workerId + " starting...");
        try {
            while (true) {
                Task task = taskQueue.getTask();
                if (task == null) break;

                processTask(task);
            }
        } catch (InterruptedException e) {
            System.err.println("Worker-" + workerId + " interrupted.");
            Thread.currentThread().interrupt();
        }
        System.out.println("Worker-" + workerId + " completing.");
    }

    private void processTask(Task task) {
        try {
            System.out.println("Worker-" + workerId + " processing " + task);
            
            // Simulate computational delay
            Thread.sleep(200 + random.nextInt(500));

            // Simulate potential error
            if (random.nextInt(10) == 0) {
                throw new ProcessingException("Random processing failure", new RuntimeException("Hardware glitch"));
            }

            String result = "Worker-" + workerId + " processed " + task + " at " + System.currentTimeMillis();
            logger.logResult(result);

        } catch (ProcessingException | IOException | InterruptedException e) {
            System.err.println("Worker-" + workerId + " encountered error on " + task + ": " + e.getMessage());
        }
    }
}

public class DataProcessor {
    public static void main(String[] args) {
        int numWorkers = 4;
        int totalTasks = 20;
        TaskQueue queue = new TaskQueue(10);
        ResultLogger logger = new ResultLogger("results_java.txt");
        ExecutorService executor = Executors.newFixedThreadPool(numWorkers);

        // Start Workers
        for (int i = 0; i < numWorkers; i++) {
            executor.execute(new Worker(i, queue, logger));
        }

        // Producer: Add tasks
        try {
            for (int i = 1; i <= totalTasks; i++) {
                queue.addTask(new Task(i, "Data-Payload-" + i));
            }
            
            // Allow processing to finish
            queue.shutdown();
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        System.out.println("System execution finished.");
    }
}