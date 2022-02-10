package vertx.map.reduce;

import io.vertx.core.*;
import io.vertx.core.eventbus.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class BenchmarkScheduler {

    public static final List<Context> CTX = new ArrayList<>();
    public static final long DATA_LENGTH = 10000;
    static final AtomicLong count = new AtomicLong();
    public static int CONCURRENT = Runtime.getRuntime().availableProcessors();
    public static final ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT);
    public static int INDEX = 0;

    public static Future<Void> initializeRunTime(Vertx vertx) {
        return Future.succeededFuture()
                .compose(v -> {
                    int poolSize = 100;
                    String poolName = "BENCHMARK-WORKER";
                    DeploymentOptions options = new DeploymentOptions();
                    options.setWorker(true);
                    options.setWorkerPoolName(poolName);
                    options.setWorkerPoolSize(poolSize);
                    options.setInstances(poolSize);
                    Future<String> future = Future.succeededFuture();
                    future = future
                            .compose(r -> vertx.deployVerticle(BenchmarkScheduler.EventBusVerticle::new, options));
                    return future;
                })
                .map(r -> null);
    }

    public static void startRunFixedThreadPool(Vertx vertx, int jobId, long dataId, Promise<Void> promise) {
        if (dataId == 0) System.out.println("[" + jobId + "] starting running");
        runFixedThreadPool(vertx)
                .onComplete(res -> {
                    if (res.succeeded()) {
                        if (dataId < DATA_LENGTH) {
                            startRunFixedThreadPool(vertx, jobId, dataId + 1, promise);
                        } else {
                            System.out.println("[" + jobId + "] complete running");
                            promise.complete();
                        }
                    } else {
                        res.cause().printStackTrace();
                    }
                });
    }

    public static void startRunWorkerContext(Vertx vertx, int jobId, long dataId, Promise<Void> promise) {
        if (dataId == 0) System.out.println("[" + jobId + "] starting running");
        runWorkerContext(vertx)
                .onComplete(res -> {
                    if (res.succeeded()) {
                        if (dataId < DATA_LENGTH) {
                            startRunWorkerContext(vertx, jobId, dataId + 1, promise);
                        } else {
                            System.out.println("[" + jobId + "] complete running");
                            promise.complete();
                        }
                    } else {
                        res.cause().printStackTrace();
                    }
                });
    }

    public static void startRunEventBus(Vertx vertx, int jobId, long dataId, Promise<Void> promise) {
        if (dataId == 0) System.out.println("[" + jobId + "] starting running");
        runEventBus(vertx)
                .onComplete(res -> {
                    if (res.succeeded()) {
                        if (dataId < DATA_LENGTH) {
                            startRunEventBus(vertx, jobId, dataId + 1, promise);
                        } else {
                            System.out.println("[" + jobId + "] complete running");
                            promise.complete();
                        }
                    } else {
                        res.cause().printStackTrace();
                    }
                });
    }

    public static Future<Void> runFixedThreadPool(Vertx vertx) {
        Promise<Void> promise = Promise.promise();
        executor.execute(() -> {
            calculate();
            promise.complete();
        });
        return promise.future();
    }

    public static Future<Void> runEventBus(Vertx vertx) {
        return vertx.eventBus().request("/test", "").map(r -> null);
    }

    public static synchronized int getNextIndex() {
        if (INDEX >= CTX.size()) INDEX = 0;
        return INDEX++;
    }

    public static Future<Void> runWorkerContext(Vertx vertx) {
        return vertx.executeBlocking(p -> {
            calculate();
            p.complete();
        }, false);
    }

    public static void calculate() {
        count.incrementAndGet();
        int a = 1;
        for (long i = 0; i < 100000L; i++) {
            a++;
        }
//        log.debug("cal");
    }

    public static void scheduleWithFixedThreadPool() {
        Vertx vertx = Vertx.vertx();
        long start = System.currentTimeMillis();
        List<Integer> jobs = IntStream.range(0, 100).boxed().collect(Collectors.toList());
        Future.succeededFuture()
                .compose(v -> initializeRunTime(vertx))
                .compose(v -> {
                    List<Future> futures = new ArrayList<>();
                    for (Integer id : jobs) {
                        Promise<Void> promise = Promise.promise();
                        startRunFixedThreadPool(vertx, id, 0, promise);
                        futures.add(promise.future());
                    }
                    return CompositeFuture.all(futures);
                })
                .onComplete(res -> {
                    if (res.succeeded()) {
                        System.out.println("all complete " + (System.currentTimeMillis() - start) + " ms used " + count.get());
                    } else {
                        res.cause().printStackTrace();
                    }
                });
    }

    public static void runOnWorkerContext() {
        Vertx vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(100));
        long start = System.currentTimeMillis();
        List<Integer> jobs = IntStream.range(0, 100).boxed().collect(Collectors.toList());
        Future.succeededFuture()
                .compose(v -> {
                    List<Future> futures = new ArrayList<>();
                    for (Integer id : jobs) {
                        Promise<Void> promise = Promise.promise();
                        startRunWorkerContext(vertx, id, 0, promise);
                        futures.add(promise.future());
                    }
                    return CompositeFuture.all(futures);
                })
                .onComplete(res -> {
                    if (res.succeeded()) {
                        System.out.println("all complete " + (System.currentTimeMillis() - start) + " ms used " + count.get());
                    } else {
                        res.cause().printStackTrace();
                    }
                });
    }

    public static void scheduleByEventBus() {
        Vertx vertx = Vertx.vertx();
        long start = System.currentTimeMillis();
        List<Integer> jobs = IntStream.range(0, 100).boxed().collect(Collectors.toList());
        Future.succeededFuture()
                .compose(v -> initializeRunTime(vertx))
                .compose(v -> {
                    List<Future> futures = new ArrayList<>();
                    for (Integer id : jobs) {
                        Promise<Void> promise = Promise.promise();
                        startRunEventBus(vertx, id, 0, promise);
                        futures.add(promise.future());
                    }
                    return CompositeFuture.all(futures);
                })
                .onComplete(res -> {
                    if (res.succeeded()) {
                        System.out.println("all complete " + (System.currentTimeMillis() - start) + " ms used");
                    } else {
                        res.cause().printStackTrace();
                    }
                });
    }

    public static void main(String[] args) {

        System.out.println("hallo");
        //fastest schedule
        scheduleWithFixedThreadPool();
        //second fast
//        runOnWorkerContext();
        //third fast
        // scheduleByEventBus();
    }

    public static class WorkerPoolVerticle extends AbstractVerticle {
        @Override
        public void start() {
            CTX.add(vertx.getOrCreateContext());
        }
    }

    public static class EventBusVerticle extends AbstractVerticle {
        @Override
        public void start() {
            vertx.eventBus().consumer("/test", this::onMessage);
        }

        private void onMessage(Message<String> msg) {
            calculate();
            msg.reply("");
        }
    }
}
