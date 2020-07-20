package com.hazelcast.jet;

import com.hazelcast.jet.grpc.greeter.GreeterGrpc;
import com.hazelcast.jet.grpc.greeter.GreeterGrpc.GreeterStub;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloReply;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloRequest;
import com.hiko.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class Test2 {

    public static final int THREADS = 1;
    public static final int BATCH = 100;
    public static final int NUM = 1;

    public static void main(String[] args) throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(THREADS);
        long start = System.currentTimeMillis();
        ChangeRequestList batch = createBatch();
        for (int i = 0; i < THREADS; i++) {

            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9000)
                                                          .directExecutor()
                                                          .usePlaintext()
                                                          .build();
            BalanceServiceGrpc.BalanceServiceStub stub = BalanceServiceGrpc.newStub(channel);
            for (int k = 0; k < 1; k++) {

                new Thread(() -> {

                    for (int j = 0; j < NUM; j++) {
                        CompletableFuture<Integer> future = new CompletableFuture<>();
                        stub.changeRequestListUnary(batch, new StreamObserver<ChangeReplyList>() {
                            @Override
                            public void onNext(ChangeReplyList value) {
                                future.complete(value.getStatusList().get(0));
                            }

                            @Override
                            public void onError(Throwable t) {
                                t.printStackTrace();
                            }

                            @Override
                            public void onCompleted() {
                                if (!future.isDone()) {
                                    future.completeExceptionally(new RuntimeException("completed when not done"));
                                }
                            }
                        });
                        try {
                            future.get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    latch.countDown();
                }).start();
            }
        }
        latch.await();
        long time = System.currentTimeMillis() - start;
        System.out.println("Took: " + time + " avg/s " + (THREADS * 100_000) / (time / 1000));
    }

    private static ChangeRequestList createBatch() {
        List<ChangeRequest> changeRequestList = new ArrayList<>();
        for (int i = 0 ; i < BATCH; i ++) {
            changeRequestList.add(ChangeRequest.newBuilder().setRid("r" + i).setUid(i%2).setAmount(i).setType(i%2).build());
        }
        return ChangeRequestList.newBuilder().addAllChangeRequests(changeRequestList).build();
    }
}
