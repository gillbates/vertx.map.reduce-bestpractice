# Sample code for fast scheduling using vertx for map-reduce like compute jobs.

## Scenario:
We are using vertx to run some backtests with large data sets for stocks. 

In production, we load the data into memory with Future from vertx, which perfectly increase the IO.

But we found that the way we schedule our calculation task also matters ...

In our production:

1, The data is loaded as a list of sequential events, e.g. 100k events. 

2, All these events are sent to 1000 StrategyCalculate Worker concurrently to do some calculations.

3, All these events are sent one by one.

4, We would rank the result after each StrategyCalculate Worker finished the calculation of his 100k events.

In order to simply and reproduce the problem, we implement this BenchmarkScheduler with 3 methods.

### scheduleWithFixedThreadPool

This will use ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT) to simlute the calculation and complete the future. 
This is the fastest way on our server, but it maybe improved?

### runOnWorkerContext

This will put the calculation job in vertx.runOnContext(), which is the 2nd fast ...

### scheduleByEventBus

This will use the traditional way eventBus/verticle to schedule the calculation, but it is the slowest one ...
