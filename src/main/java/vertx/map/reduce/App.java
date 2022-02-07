package vertx.map.reduce;

import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

@Log4j2
public class App {

    public static final List<Context> CTX = new ArrayList<>();
    public static final long DATA_LENGTH = 10000;
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
                    Future<String> future = Future.succeededFuture();
                    for (int i = 0; i < poolSize; i++) {
                        future = future
                                .compose(r -> vertx.deployVerticle(new WorkerPoolVerticle(), options))
                                .compose(r -> vertx.deployVerticle(new EventLoopVerticle()));
                    }
                    return future;
                })
                .map(r -> null);
    }

    public static void startRunFixedThreadPool(Vertx vertx, int jobId, long dataId, Promise<Void> promise) {
        if (dataId == 0) log.debug("[{}] starting running", jobId);
        runFixedThreadPool(vertx)
                .onComplete(res -> {
                    if (res.succeeded()) {
                        if (dataId < DATA_LENGTH) {
                            startRunFixedThreadPool(vertx, jobId, dataId + 1, promise);
                        } else {
                            log.debug("[{}] complete running", jobId);
                            promise.complete();
                        }
                    } else {
                        res.cause().printStackTrace();
                    }
                });
    }

    public static void startRunWorkerContext(Vertx vertx, int jobId, long dataId, Promise<Void> promise) {
        if (dataId == 0) log.debug("[{}] starting running", jobId);
        runWorkerContext(vertx)
                .onComplete(res -> {
                    if (res.succeeded()) {
                        if (dataId < DATA_LENGTH) {
                            startRunWorkerContext(vertx, jobId, dataId + 1, promise);
                        } else {
                            log.debug("[{}] complete running", jobId);
                            promise.complete();
                        }
                    } else {
                        res.cause().printStackTrace();
                    }
                });
    }

    public static void startRunEventBus(Vertx vertx, int jobId, long dataId, Promise<Void> promise) {
        if (dataId == 0) log.debug("[{}] starting running", jobId);
        runEventBus(vertx)
                .onComplete(res -> {
                    if (res.succeeded()) {
                        if (dataId < DATA_LENGTH) {
                            startRunEventBus(vertx, jobId, dataId + 1, promise);
                        } else {
                            log.debug("[{}] complete running", jobId);
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
        Context currentCtx = vertx.getOrCreateContext();
        Context workerCtx = CTX.get(getNextIndex());
        Promise<Void> promise = Promise.promise();
        workerCtx.runOnContext(v -> {
            calculate();
            currentCtx.runOnContext(r -> promise.complete());
        });
        return promise.future();
    }

    public static void calculate() {
        int a = 1;
        for (long i = 0; i < 100000L; i++) {
            a++;
        }
    }

    public static void scheduleWithFixedThreadPool() {
        Vertx vertx = Vertx.vertx();
        long start = System.currentTimeMillis();
        List<Integer> jobs = IntStream.range(0, 100).boxed().toList();
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
                        log.debug("all complete {} ms used", System.currentTimeMillis() - start);
                    } else {
                        res.cause().printStackTrace();
                    }
                });
    }

    public static void runOnWorkerContext() {
        Vertx vertx = Vertx.vertx();
        long start = System.currentTimeMillis();
        List<Integer> jobs = IntStream.range(0, 100).boxed().toList();
        Future.succeededFuture()
                .compose(v -> initializeRunTime(vertx))
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
                        log.debug("all complete {} ms used", System.currentTimeMillis() - start);
                    } else {
                        res.cause().printStackTrace();
                    }
                });
    }

    public static void scheduleByEventBus() {
        Vertx vertx = Vertx.vertx();
        long start = System.currentTimeMillis();
        List<Integer> jobs = IntStream.range(0, 100).boxed().toList();
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
                        log.debug("all complete {} ms used", System.currentTimeMillis() - start);
                    } else {
                        res.cause().printStackTrace();
                    }
                });
    }



    public static class WorkerPoolVerticle extends AbstractVerticle {
        @Override
        public void start() {
            CTX.add(vertx.getOrCreateContext());
        }
    }

    public static class EventLoopVerticle extends AbstractVerticle {
        @Override
        public void start() {
            vertx.eventBus().consumer("/test", this::onMessage);
        }

        private void onMessage(Message<String> msg) {
            calculate();
            msg.reply("");
        }
    }

    public static void main(String[] args) {

        log.debug("hello world");
        //fastest schedule
        scheduleWithFixedThreadPool();
        //second fast
//        runOnWorkerContext();
        //third fast
//        scheduleByEventBus();
    }
}
