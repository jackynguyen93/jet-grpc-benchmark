package com.hazelcast.jet;

import com.google.common.util.concurrent.UncaughtExceptionHandlers;
import com.hazelcast.jet.dto.Order;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloReply;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloReplyList;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloReplyList.Builder;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloRequest;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloRequestList;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
//import io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoopGroup;
//import io.grpc.netty.shaded.io.netty.channel.epoll.EpollServerSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hello world!
 */
public class GrpcServer {

    public static final int WAIT_TIME = 10;
    static JedisHelper jedisHelper = new JedisHelper();

    public static void main(String[] args) throws Exception {
        jedisHelper.initialize();
        Server server = createServer("direct", new GreeterServiceImpl());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.shutdownNow().awaitTermination();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        server.awaitTermination();
    }

    private static Server createServer(String executor, BindableService service) throws IOException {
        final int port = 8081;
        int processors = Runtime.getRuntime().availableProcessors();
        NettyServerBuilder builder = NettyServerBuilder.forPort(port);

        ThreadFactory tf = new DefaultThreadFactory("server-elg-", true);
        builder.bossEventLoopGroup(new NioEventLoopGroup(1, tf));
        builder.workerEventLoopGroup(new NioEventLoopGroup(0, tf));
        builder.channelType(NioServerSocketChannel.class);

        if (executor.equals("direct")) {
            System.out.println("Using directExecutor()");
            builder.directExecutor();
        }
        if (executor.equals("fixed-thread")) {
            System.out.println("Using Executors.newFixedThreadPool(processors) as executor()");
            builder.executor(Executors.newFixedThreadPool(processors));
        }
        if (executor.equals("fork-join")) {
            System.out.println("Using new ForkJoinPool(processors) as executor()");

            // Taken from https://github.com/grpc/grpc-java/blob/master/benchmarks/src/main/java/io/grpc/benchmarks/qps/AsyncServer.java#L177
            builder.executor(new ForkJoinPool(Runtime.getRuntime().availableProcessors(),
                    new ForkJoinWorkerThreadFactory() {
                        final AtomicInteger num = new AtomicInteger();
                        @Override
                        public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                            ForkJoinWorkerThread thread =
                                    ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                            thread.setDaemon(true);
                            thread.setName("grpc-server-app-" + "-" + num.getAndIncrement());
                            return thread;
                        }
                    }, UncaughtExceptionHandlers.systemExit(), true /* async */));
        }
        builder.addService(service);
        Server server = builder.build();
        server.start();
        System.out.println("Server started at " + port);
        return server;
    }

    private static class GreeterServiceImpl extends com.hazelcast.jet.grpc.greeter.GreeterGrpc.GreeterImplBase {

        private final String workloadType;

        private GreeterServiceImpl() {
            this.workloadType = "No";
        }

        @Override
        public void sayHelloUnary(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            jedisHelper.subtractBalance(new Order("u1", 1D, "u1", new Date()));

            HelloReply reply = HelloReply.newBuilder()
                    .setValue(1)
                    .build();
            //waitIfNeeded();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void sayHelloListUnary(HelloRequestList request, StreamObserver<HelloReplyList> responseObserver) {
            Builder builder = HelloReplyList.newBuilder();
            request.getValueList()
                   .stream()
                   .map(item -> {
                      // waitIfNeeded();
                       jedisHelper.subtractBalance(new Order("u1", 1D, "u1", new Date()));
                       return item * 2;
                   })
                   .forEach(builder::addValue);

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<HelloRequest> sayHelloBidirectional(StreamObserver<HelloReply> responseObserver) {

            return new StreamObserver<HelloRequest>() {

                @Override
                public void onNext(HelloRequest value) {
                    HelloReply reply = HelloReply.newBuilder()
                                                 .setValue(value.getValue() * 2)
                                                 .build();
                    //waitIfNeeded();
                    jedisHelper.subtractBalance(new Order("u1", 1D, "u1", new Date()));
                    responseObserver.onNext(reply);
                }

                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }

        @Override
        public StreamObserver<HelloRequestList> sayHelloListBidirectional(StreamObserver<HelloReplyList> responseObserver) {

            return new StreamObserver<HelloRequestList>() {

                @Override
                public void onNext(HelloRequestList value) {
                    Builder builder = HelloReplyList.newBuilder();
                    value.getValueList()
                         .stream()
                         .map(item -> {
                             //waitIfNeeded();
                             jedisHelper.subtractBalance(new Order("u1", 1D, "u1", new Date()));
                             return item * 2;
                         })
                         .forEach(builder::addValue);

                    responseObserver.onNext(builder.build());
                }

                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }

        private void waitIfNeeded() {
            if (workloadType.equals("blocking-wait")) {
                try {
                    Thread.sleep(WAIT_TIME);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            if (workloadType.equals("busy-wait")) {
                long end = System.nanoTime() + Duration.ofMillis(WAIT_TIME).toNanos();
                while ((end - System.nanoTime()) > 0) {
                }
            }
        }
    }

}
