package com.hazelcast.jet;

import com.hazelcast.jet.grpc.greeter.GreeterGrpc;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by rayt on 5/16/16.
 */
public class GrpcClient {
    public static void main(String[] args) throws InterruptedException {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080)
                .directExecutor()
                .keepAliveTime(1, TimeUnit.MINUTES)
                .keepAliveTimeout(5, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true)
                .usePlaintext()
                .build();

        GreeterGrpc.GreeterStub stub = GreeterGrpc.newStub(channel);

        GreeterOuterClass.HelloRequest request = GreeterOuterClass.HelloRequest.newBuilder().setValue(4).build();
        CompletableFuture<Integer> future = new CompletableFuture<>();
        stub.sayHelloUnary(request, new StreamObserver<GreeterOuterClass.HelloReply>() {
            @Override
            public void onNext(GreeterOuterClass.HelloReply value) {
                future.complete(value.getValue());
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
        channel.shutdown();
    }
}