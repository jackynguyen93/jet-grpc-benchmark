package com.hazelcast.jet;

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MetricNames;
import com.hazelcast.jet.grpc.GrpcService;
import com.hazelcast.jet.grpc.GrpcServices;
import com.hazelcast.jet.grpc.greeter.GreeterGrpc;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloReply;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloReplyList;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloRequest;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloRequestList;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hiko.proto.BalanceServiceGrpc;
import com.hiko.proto.ChangeReplyList;
import com.hiko.proto.ChangeRequest;
import com.hiko.proto.ChangeRequestList;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.hazelcast.jet.grpc.GrpcServices.bidirectionalStreamingService;
import static com.hazelcast.jet.grpc.GrpcServices.unaryService;

public class BatchBenchmarkJob {

    private static String FORMAT_HEADER = "%-30s %-30s %-30s %30s";
    private static String FORMAT = "%-30s %-30d %-30d %30.2f";
    private final JetInstance jet;
    private final String runId;

    private String pipelineType;

    private String host;
    private int port;
    private String executor;
    private int jobBatchSize;
    private int maxConcurrentOps;
    private int localParallelism;
    private int mapBatchSize;
    private Pipeline pipeline;
    private static int NUMBER_USER = 50000;

    public BatchBenchmarkJob(JetInstance jet, String host, int port, String executor, int jobBatchSize, int maxConcurrentOps, int localParallelism,
                             int mapBatchSize) {
        this.runId = UuidUtil.newUnsecureUuidString();
        this.jet = jet;
        this.host = host;
        this.port = port;
        this.executor = executor;
        this.jobBatchSize = jobBatchSize;
        this.maxConcurrentOps = maxConcurrentOps;
        this.localParallelism = localParallelism;
        this.mapBatchSize = mapBatchSize;
    }

    private String runAndReport() {
        JobConfig config = new JobConfig();
        config.setStoreMetricsAfterJobCompletion(true);
        config.addClass(BatchBenchmarkJob.class);
        config.addPackage("com.hazelcast.jet.grpc.greeter");

        Job job = jet.newJob(pipeline, config);
        job.join();

        JobMetrics metrics = job.getMetrics();
        List<Measurement> start = metrics.get(MetricNames.EXECUTION_START_TIME);
        List<Measurement> completion = metrics.get(MetricNames.EXECUTION_COMPLETION_TIME);
        long timeMs = completion.get(0).value() - start.get(0).value();

        double itemsPerSecond = (double) jobBatchSize / timeMs * 1000;

        return String.format(FORMAT, pipelineType, maxConcurrentOps, localParallelism, itemsPerSecond);
    }

    public BatchBenchmarkJob withUnaryPipeline() {
        pipelineType = "unary";
        pipeline = unary(runId + pipelineType, host, port, maxConcurrentOps, localParallelism, executor, jobBatchSize);
        return this;
    }

    public BatchBenchmarkJob withBidirectinalStreamingPipeline() {
        pipelineType = "bidi";
        pipeline = bidirectinalStreaming(runId + pipelineType, host, port, maxConcurrentOps,
                localParallelism, executor, jobBatchSize);
        return this;
    }

    public BatchBenchmarkJob withUnaryBatchPipeline() {
        pipelineType = "unary-batch";
        pipeline = unaryBatch(runId + pipelineType, host, port, mapBatchSize,
                localParallelism, executor, jobBatchSize);
        return this;
    }

    public BatchBenchmarkJob withBidirectionalStreamingBatchPipeline() {
        pipelineType = "bidi-batch";
        pipeline = bidirectionalStreamingBatch(runId + pipelineType, host, port, mapBatchSize, localParallelism,
                executor, jobBatchSize);
        return this;
    }

    public static void main(String[] args) {
        JetInstance jet = Jet.bootstrappedInstance();

        String host = "localhost";//Utils.getProp("localhost");
        int port = 9000; // Utils.getIntProp("8080");
        String executor = "direct" ; //Utils.getProp("executor");
        int mapBatchSize = 1024; // Utils.getIntProp("mapBatchSize", "1024");
        int jobBatchSize = 1000000; //Utils.getIntProp("jobBatchSize", "50000");
        int batchMultiplier = 10; //Utils.getIntProp("multiplier", "10");
        int[] maxConcurrentOpsValues = new int[] {128} ;//Utils.getIntPropArray("maxConcurrentOps", "4");
        int[] localParallelismValues =  new int[] {108}; //Utils.getIntPropArray("localParallelism",String.valueOf(Runtime.getRuntime().availableProcessors()));

        List<String> results = new ArrayList<>();

        for (int maxConcurrentOps : maxConcurrentOpsValues) {
            for (int localParallelism : localParallelismValues) {
                String result;
//               result = new BatchBenchmarkJob(jet, host, port, executor, jobBatchSize, maxConcurrentOps,
//                        localParallelism, mapBatchSize)
//                        .withUnaryPipeline()
//                        .runAndReport();
//                results.add(result);
//
//                result = new BatchBenchmarkJob(jet, host, port, executor, jobBatchSize, maxConcurrentOps,
//                        localParallelism, mapBatchSize)
//                        .withBidirectinalStreamingPipeline()
//                        .runAndReport();
//                results.add(result);

                result = new BatchBenchmarkJob(jet, host, port, executor, jobBatchSize * batchMultiplier, maxConcurrentOps,
                        localParallelism, mapBatchSize)
                        .withUnaryBatchPipeline()
                        .runAndReport();
                results.add(result);
//
//                result = new BatchBenchmarkJob(jet, host, port, executor, jobBatchSize * batchMultiplier, maxConcurrentOps,
//                        localParallelism, mapBatchSize)
//                        .withBidirectionalStreamingBatchPipeline()
//                        .runAndReport();
//                results.add(result);
            }
        }

        jet.shutdown();

        try (FileOutputStream out = new FileOutputStream("results.txt"); PrintWriter writer =
                new PrintWriter(out)) {

            String header = String.format(FORMAT_HEADER, "Pipeline", "maxConcurrentOps", "localParallelism", "itemsPerSecond");
            System.out.println(header);
            writer.println(header);

            for (String result : results) {
                System.out.println(result);
                writer.println(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Pipeline unary(String runId, String host, int port, int maxConcurrentOps, int localParallelism,
                                 String executor, int jobBatchSize) {
        var unaryService = GrpcServices.<HelloRequest, HelloReply>unaryService(
                () -> Utils.createChannelBuilder(host, port, executor),
                channel -> GreeterGrpc.newStub(channel)::sayHelloUnary
        );

        Pipeline p = Pipeline.create();
        BatchStage<Integer> stage = p.readFrom(intSource(jobBatchSize));
        stage.mapUsingServiceAsync(unaryService,
                maxConcurrentOps, true,
                (service, item) -> service.call(HelloRequest.newBuilder().setValue(item).build()))
             .setLocalParallelism(localParallelism)
             .aggregate(AggregateOperations.counting())
             .writeTo(Sinks.observable(runId));

        return p;
    }

    public static Pipeline unaryBatch(String runId, String host, int port, int batchSize, int localParallelism,
                                      String executor, int jobBatchSize) {
        ServiceFactory<?, ? extends GrpcService<ChangeRequestList, ChangeReplyList>> unaryService = unaryService(
                () -> Utils.createChannelBuilder(host, port, executor),
                channel -> BalanceServiceGrpc.newStub(channel)::changeRequestListUnary
        );

        Pipeline p = Pipeline.create();

        p.readFrom(intSource(jobBatchSize))
         .mapUsingServiceAsyncBatched(unaryService, batchSize,
                 (service, items) -> {
                    List<ChangeRequest> changeRequests = items.stream().map(item -> {
                                int type = Math.random() > 0.5 ? 1 : 2;
                                String rid = UUID.randomUUID().toString();
                                return  ChangeRequest.newBuilder().setRid(rid).setAmount(type == 1 ? 10 : 1).setType(type).setUid(item % NUMBER_USER).build();
                            }
                           )
                            .collect(Collectors.toList());

                    return service.call(ChangeRequestList.newBuilder().addAllChangeRequests(changeRequests).build())
                             .thenApply(ChangeReplyList::getStatusList);
                 }
         )
         .setLocalParallelism(localParallelism)
         .aggregate(AggregateOperations.counting())
         .writeTo(Sinks.observable(runId));

        return p;
    }

    public static Pipeline bidirectinalStreaming(String runId, String host, int port,
                                                 int maxConcurrentOps,
                                                 int localParallelism, String executor, int jobBatchSize) {
        ServiceFactory<?, ? extends GrpcService<HelloRequest, HelloReply>> bidiService = bidirectionalStreamingService(
                () -> Utils.createChannelBuilder(host, port, executor),
                channel -> GreeterGrpc.newStub(channel)::sayHelloBidirectional
        );

        Pipeline p = Pipeline.create();
        p.readFrom(intSource(jobBatchSize))
         .mapUsingServiceAsync(bidiService,
                 maxConcurrentOps, true,
                 (service, item) -> service.call(HelloRequest.newBuilder().setValue(item).build()))
         .setLocalParallelism(localParallelism)
         .aggregate(AggregateOperations.counting())
         .writeTo(Sinks.observable(runId));

        return p;
    }

    public static Pipeline bidirectionalStreamingBatch(String runId, String host, int port,
                                                       int batchSize, int localParallelism, String executor,
                                                       int jobBatchSize) {
        ServiceFactory<?, ? extends GrpcService<ChangeRequestList, ChangeReplyList>> bidiService =
                bidirectionalStreamingService(
                        () -> Utils.createChannelBuilder(host, port, executor),
                        channel -> BalanceServiceGrpc.newStub(channel)::changeRequestListBidirectional
                );

        Pipeline p = Pipeline.create();
        p.readFrom(intSource(jobBatchSize))
         .mapUsingServiceAsyncBatched(bidiService,
                 batchSize,
                 (service, items) -> {
                     List<ChangeRequest> changeRequests = items.stream().map(item ->
                             ChangeRequest.newBuilder().setRid("r" + item).setAmount(1).setType(item % 2).setUid(item % NUMBER_USER).build())
                             .collect(Collectors.toList());
                     CompletableFuture<ChangeReplyList> future =
                             service.call(ChangeRequestList.newBuilder().addAllChangeRequests(changeRequests).build());
                     return future.thenApply(ChangeReplyList::getStatusList);
                 })
         .setLocalParallelism(localParallelism)
         .aggregate(AggregateOperations.counting())
         .writeTo(Sinks.observable(runId));

        return p;
    }

    private static BatchSource<Integer> intSource(int items) {
        return SourceBuilder.batch("int-source", context -> new AtomicInteger())
                .<Integer>fillBufferFn((aint, buffer) -> {
                    int next = aint.get();
                    if (next < items) {
                        for (int i = 0; i < 1000; i++) {
                            buffer.add(aint.getAndIncrement());
                        }
                    } else {
                        buffer.close();
                    }
                })
                .build();
    }


}
