/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.common.ServiceThread;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

import java.net.SocketAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

public abstract class NettyRemotingAbstract {

    /**
     * Remoting logger instance.
     */
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    /**
     * Semaphore to limit maximum number of on-going one-way requests, which protects system memory footprint.
     */
    protected final Semaphore semaphoreOneway;

    /**
     * Semaphore to limit maximum number of on-going asynchronous requests, which protects system memory footprint.
     */
    protected final Semaphore semaphoreAsync;

    /**
     * 请求映射表。 opaque 和 ResponseFuture
     * 会有定时任务来扫描这个表，把超时的任务移除
     *
     * @see NettyRemotingAbstract#scanResponseTable() 扫描的具体代码
     * @see NettyRemotingServer#start() 配置定时任务的地方
     * This map caches all on-going requests.
     */
    protected final ConcurrentMap<Integer /* opaque */, ResponseFuture> responseTable =
            new ConcurrentHashMap<Integer, ResponseFuture>(256);

    /**
     * 业务代码(Code) :  pair 。 其实就是获取 处理器 和 线程池资源。
     * <p>
     * This container holds all processors per request code, aka, for each incoming request, we may look up the
     * responding processor in this map to handle the request.
     */
    protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable =
            new HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>>(64);

    /**
     * Executor to feed netty events to user defined {@link ChannelEventListener}.
     */
    protected final NettyEventExecutor nettyEventExecutor = new NettyEventExecutor();

    /**
     * The default request processor to use in case there is no exact match in {@link #processorTable} per request code.
     */
    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;

    /**
     * SSL context via which to create {@link SslHandler}.
     */
    protected volatile SslContext sslContext;

    /**
     * custom rpc hooks
     */
    protected List<RPCHook> rpcHooks = new ArrayList<RPCHook>();


    static {
        NettyLogger.initNettyLogger();
    }

    /**
     * Constructor, specifying capacity of one-way and asynchronous semaphores.
     *
     * @param permitsOneway Number of permits for one-way requests.
     * @param permitsAsync  Number of permits for asynchronous requests.
     */
    public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
    }

    /**
     * Custom channel event listener.
     *
     * @return custom channel event listener if defined; null otherwise.
     */
    public abstract ChannelEventListener getChannelEventListener();

    /**
     * Put a netty event to the executor.
     *
     * @param event Netty event instance.
     */
    public void putNettyEvent(final NettyEvent event) {
        this.nettyEventExecutor.putNettyEvent(event);
    }

    /**
     * 处理网络请求的入口
     * <p>
     * Entry of incoming command processing.
     *
     * <p>
     * <strong>Note:</strong>
     * The incoming remoting command may be
     * <ul>
     * <li>An inquiry request from a remote peer component;</li>
     * <li>A response to a previous request issued by this very participant.</li>
     * </ul>
     * </p>
     *
     * @param ctx Channel handler context.
     * @param msg incoming remoting command.
     * @throws Exception if there were any error while processing the incoming command.
     */
    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        final RemotingCommand cmd = msg;
        if (cmd != null) {
            switch (cmd.getType()) {
                case REQUEST_COMMAND: // 客户端发起的请求，走这里
                    processRequestCommand(ctx, cmd);
                    break;
                case RESPONSE_COMMAND: // 客户端响应的请求，走这里
                    processResponseCommand(ctx, cmd);
                    break;
                default:
                    break;
            }
        }
    }

    protected void doBeforeRpcHooks(String addr, RemotingCommand request) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook : rpcHooks) {
                rpcHook.doBeforeRequest(addr, request);
            }
        }
    }

    protected void doAfterRpcHooks(String addr, RemotingCommand request, RemotingCommand response) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook : rpcHooks) {
                rpcHook.doAfterResponse(addr, request, response);
            }
        }
    }


    /**
     * 客户端发起的请求，走这里
     * <p>
     * Process incoming request command issued by remote peer.
     *
     * @param ctx channel handler context.
     * @param cmd request command.
     */
    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        // 根据业务代码 找到 合适的处理器， 和线程池资源 pair
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());

        // 如果未找到 code对应的pair, 则使用缺省处理器。 nameServer 并未注册code对应的处理器，使用缺省。
        final Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? this.defaultRequestProcessor : matched;
        // 获取请求ID
        final int opaque = cmd.getOpaque();

        if (pair != null) {
            // 封装一个 runnable, 核心逻入口。
            // 先看下面的流程
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    try {
                        // RPC hook before
                        doBeforeRpcHooks(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd);

                        // callback , 封装 响应客户端的逻辑。
                        final RemotingResponseCallback callback = new RemotingResponseCallback() {
                            @Override
                            public void callback(RemotingCommand response) {
                                // RPC hook after
                                doAfterRpcHooks(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd, response);

                                if (!cmd.isOnewayRPC()) {
                                    if (response != null) {
                                        // 将请求id 设置到 response.opaque内，客户端根据 opaque 值，在responseFutureTable 找到 responseFuture 完成结果的交互。
                                        response.setOpaque(opaque);
                                        // 设置当前请求 是响应类型。
                                        response.markResponseType();
                                        try {
                                            // 将数据 交给 netty IO 线程，完成 数据 写和刷。
                                            ctx.writeAndFlush(response);
                                        } catch (Throwable e) {
                                            log.error("process request over, but response failed", e);
                                            log.error(cmd.toString());
                                            log.error(response.toString());
                                        }
                                    } else {
                                    }
                                }
                            }
                        };

                        // nameServer 使用的是 DefaultRequestProcessor, 是一个 AsyncNettyRequestProcessor 子类。会进入条件。
                        if (pair.getObject1() instanceof AsyncNettyRequestProcessor) {
                            // DefaultRequestProcessor
                            AsyncNettyRequestProcessor processor = (AsyncNettyRequestProcessor) pair.getObject1();

                            /**
                             *异步处理请求，  AsyncNettyRequestProcessor 中这个方法的逻辑其实是同步代码，除非子类覆盖这个方法。
                             *然而 DefaultRequestProcessor 没有复写方法，
                             *
                             * @param ctx              ctx
                             * @param request          客户端数据封装对象
                             * @param responseCallback callback 封装响应客户端的逻辑
                             * @throws Exception
                             */
                            processor.asyncProcessRequest(ctx, cmd, callback);
                        } else {
                            NettyRequestProcessor processor = pair.getObject1();
                            RemotingCommand response = processor.processRequest(ctx, cmd);
                            callback.callback(response);
                        }
                    } catch (Throwable e) {
                        log.error("process request exception", e);
                        log.error(cmd.toString());

                        if (!cmd.isOnewayRPC()) {
                            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                                    RemotingHelper.exceptionSimpleDesc(e));
                            response.setOpaque(opaque);
                            ctx.writeAndFlush(response);
                        }
                    }
                }
            };

            // 失败策略
            if (pair.getObject1().rejectRequest()) {
                final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                        "[REJECTREQUEST]system busy, start flow control for a while");
                response.setOpaque(opaque);
                ctx.writeAndFlush(response);
                return;
            }

            try {
                // 将 runnable 和 channel ，请求cmd 一起封装成RequestTask 对象。
                final RequestTask requestTask = new RequestTask(run, ctx.channel(), cmd);

                // 注意，这里从IO线程 切换到 业务线程了！！！
                // 获取处理器对应的线程池，将task提交， task执行时，会执行 runnable.run();
                pair.getObject2().submit(requestTask);
            } catch (RejectedExecutionException e) {
                if ((System.currentTimeMillis() % 10000) == 0) {
                    log.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel())
                            + ", too many requests and system thread pool busy, RejectedExecutionException "
                            + pair.getObject2().toString()
                            + " request code: " + cmd.getCode());
                }

                if (!cmd.isOnewayRPC()) {
                    final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                            "[OVERLOAD]system busy, start flow control for a while");
                    response.setOpaque(opaque);
                    ctx.writeAndFlush(response);
                }
            }
        } else {
            String error = " request type " + cmd.getCode() + " not supported";
            final RemotingCommand response =
                    RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            response.setOpaque(opaque);
            ctx.writeAndFlush(response);
            log.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
        }
    }

    /**
     * Process response from remote peer to the previous issued requests.
     *
     * @param ctx channel handler context.
     * @param cmd response command instance.
     */
    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        final int opaque = cmd.getOpaque();
        final ResponseFuture responseFuture = responseTable.get(opaque);
        if (responseFuture != null) {
            responseFuture.setResponseCommand(cmd);

            responseTable.remove(opaque);

            if (responseFuture.getInvokeCallback() != null) {
                executeInvokeCallback(responseFuture);
            } else {
                responseFuture.putResponse(cmd);
                responseFuture.release();
            }
        } else {
            log.warn("receive response, but not matched any request, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            log.warn(cmd.toString());
        }
    }

    /**
     * Execute callback in callback executor. If callback executor is null, run directly in current thread
     */
    private void executeInvokeCallback(final ResponseFuture responseFuture) {
        boolean runInThisThread = false;
        ExecutorService executor = this.getCallbackExecutor();
        if (executor != null) {
            try {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            responseFuture.executeInvokeCallback();
                        } catch (Throwable e) {
                            log.warn("execute callback in executor exception, and callback throw", e);
                        } finally {
                            responseFuture.release();
                        }
                    }
                });
            } catch (Exception e) {
                runInThisThread = true;
                log.warn("execute callback in executor exception, maybe executor busy", e);
            }
        } else {
            runInThisThread = true;
        }

        if (runInThisThread) {
            try {
                responseFuture.executeInvokeCallback();
            } catch (Throwable e) {
                log.warn("executeInvokeCallback Exception", e);
            } finally {
                responseFuture.release();
            }
        }
    }


    /**
     * Custom RPC hook.
     * Just be compatible with the previous version, use getRPCHooks instead.
     */
    @Deprecated
    protected RPCHook getRPCHook() {
        if (rpcHooks.size() > 0) {
            return rpcHooks.get(0);
        }
        return null;
    }

    /**
     * Custom RPC hooks.
     *
     * @return RPC hooks if specified; null otherwise.
     */
    public List<RPCHook> getRPCHooks() {
        return rpcHooks;
    }


    /**
     * This method specifies thread pool to use while invoking callback methods.
     *
     * @return Dedicated thread pool instance if specified; or null if the callback is supposed to be executed in the
     * netty event-loop thread.
     */
    public abstract ExecutorService getCallbackExecutor();

    /**
     * 扫描 responseTable 表，将过期的responseFuture 移除。
     *
     * <p>
     * This method is periodically invoked to scan and expire deprecated request.
     * </p>
     */
    public void scanResponseTable() {
        final List<ResponseFuture> rfList = new LinkedList<ResponseFuture>();
        Iterator<Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();

            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
                rep.release();
                it.remove();
                rfList.add(rep);
                log.warn("remove timeout request, " + rep);
            }
        }

        for (ResponseFuture rf : rfList) {
            try {
                executeInvokeCallback(rf);
            } catch (Throwable e) {
                log.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }

    /**
     * 具体代码
     *
     * @param channel       客户端channel
     * @param request       网络请求对象，remotingCommand
     * @param timeoutMillis 超时时长
     * @return
     * @throws InterruptedException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     */
    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        // 请求id
        final int opaque = request.getOpaque();

        try {

            /**
             * 因为要等客户端回应，封装了一个future。 用来处理同步和异步请求。
             * 是网络请求的核心
             *
             * @param channel        客户端 channel
             * @param opaque         请求id
             * @param timeoutMillis  超时时间
             */
            final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis, null, null);
            // 放入 映射表
            this.responseTable.put(opaque, responseFuture);

            // 获取客户端的地址信息
            final SocketAddress addr = channel.remoteAddress();

            /**
             * 将数据写入 客户端 channel
             * 注意， 这里  writeAndFlush(request) 相当于发送请求。
             *            addListener(xxx)       相当于设置回调。
             * 然后下面的 responseFuture.waitResponse(timeoutMillis); 进行阻塞，等待
             *
             */
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (f.isSuccess()) {
                        // 成功就返回
                        responseFuture.setSendRequestOK(true);
                        return;
                    } else {
                        // 失败
                        responseFuture.setSendRequestOK(false);
                    }


                    // 下面都都是失败逻辑
                    // 将当前请求的 responseFuture 从映射表 移除。
                    responseTable.remove(opaque);
                    // 设置失败原因
                    responseFuture.setCause(f.cause());
                    // 返回null， 调用者会处理 为null的情况。
                    // 这个方法执行完毕后，会调用  this.countDownLatch.countDown(); ，唤醒下面业务线程
                    responseFuture.putResponse(null);
                    log.warn("send a request command to channel <" + addr + "> failed.");
                }
            });

            // 业务线程 在这里进入阻塞，挂起状态。
            // 内部有个countDownLatch。
            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);

            // 线程执行到这里，有两种情况。
            // 1. 成功，客户端返回数据了，IO线程 将业务线程唤醒、
            // 2. 超时，
            if (null == responseCommand) {
                // 执行到这里，说明 超时。。 或者其他异常情况。 这里可能是超时
                if (responseFuture.isSendRequestOK()) {
                    throw new RemotingTimeoutException(RemotingHelper.parseSocketAddressAddr(addr), timeoutMillis,
                            responseFuture.getCause());
                } else {
                    // 这里就是纯纯的请求失败了。会给出失败原因
                    throw new RemotingSendRequestException(RemotingHelper.parseSocketAddressAddr(addr), responseFuture.getCause());
                }
            }
            // 返回 客户端 的 结果
            return responseCommand;
        } finally {
            // 请求结束，移除 映射表
            this.responseTable.remove(opaque);
        }
    }


    /**
     * /**
     * 异步请求具体代码
     *
     * @param channel        客户单channel
     * @param request        网络请求对象 RemotingCommand
     * @param timeoutMillis  超时时长
     * @param invokeCallback 请求结果回调处理对象。
     * @throws InterruptedException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis,
                                final InvokeCallback invokeCallback)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        // 开始时间
        long beginStartTime = System.currentTimeMillis();
        // 请求 ID
        final int opaque = request.getOpaque();
        /*
         * 信号量，如果获取失败，说明当前并发大,失败就报错了，放弃了
         *
         *     // 服务端 单向访问 客户端时的并发控制、
         *     private int serverOnewaySemaphoreValue = 256;
         *
         *     // 服务端 异步访问 客户端时的并发限制。
         *     private int serverAsyncSemaphoreValue = 64;
         */
        boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            // 执行到这里，说明当前线程可以发起请求，服务器请求客户端的 并发 没有到达上限。
            // once 对象 封装了 释放信号量的操作
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);
            // 计算 执行到这一步 为止。 耗费的时间。
            long costTime = System.currentTimeMillis() - beginStartTime;

            // 条件成立的话， 说明已经超时了，就不用发起rpc了。
            if (timeoutMillis < costTime) {
                once.release();
                throw new RemotingTimeoutException("invokeAsyncImpl call timeout");
            }


            /**
             *和 invokeSyncImpl 一样的。不过多穿了参数
             *
             * @param channel        客户端 channel
             * @param opaque         请求id
             * @param timeoutMillis  超时时间
             * @param invokeCallback 回调处理对象
             * @param once           信号量释放操作封装对象
             */
            final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis - costTime, invokeCallback, once);
            // 将 responseFuture 加入 响应 映射表 。 key其实是 RequestId
            this.responseTable.put(opaque, responseFuture);
            try {
                // 1. 业务线程 将数据 交给netty, netty io 线程 接管 写 和刷 数据的操作
                // 2. 注册 写刷 操作监听器， 监听器由 IO线程回调。
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        if (f.isSuccess()) {
                            // 写刷成功，设置 responseFuture 发生状态为 true
                            responseFuture.setSendRequestOK(true);
                            return;
                        }
                        // 执行到这里，就是发送失败..
                        requestFail(opaque);
                        log.warn("send a request command to channel <{}> failed.", RemotingHelper.parseChannelRemoteAddr(channel));
                    }
                });
            } catch (Exception e) {
                responseFuture.release();
                log.warn("send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel) + "> Exception", e);
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
            // 没有返回结果。 因为最终会回调  invokeCallback

        } else {
            // 执行到这里，说明获取 信号量失败，说明 当前服务器 并发比较高...
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
            } else {
                String info =
                        String.format("invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                                timeoutMillis,
                                this.semaphoreAsync.getQueueLength(),
                                this.semaphoreAsync.availablePermits()
                        );
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

    /**
     * 请求失败的处理逻辑
     *
     * @param opaque
     */
    private void requestFail(final int opaque) {
        ResponseFuture responseFuture = responseTable.remove(opaque);
        if (responseFuture != null) {
            responseFuture.setSendRequestOK(false);
            responseFuture.putResponse(null);
            try {
                // 调用 回调处理器对象，异步 使用 CallbackExecutor， 公共线程池
                executeInvokeCallback(responseFuture);
            } catch (Throwable e) {
                log.warn("execute callback in requestFail, and callback throw", e);
            } finally {
                responseFuture.release();
            }
        }
    }

    /**
     * mark the request of the specified channel as fail and to invoke fail callback immediately
     *
     * @param channel the channel which is close already
     */
    protected void failFast(final Channel channel) {
        Iterator<Entry<Integer, ResponseFuture>> it = responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> entry = it.next();
            if (entry.getValue().getProcessChannel() == channel) {
                Integer opaque = entry.getKey();
                if (opaque != null) {
                    requestFail(opaque);
                }
            }
        }
    }


    /**
     * 单行请求 不关注返回结果
     *
     * @param channel       客户端channel
     * @param request       网络请求对象 RemotingCommand
     * @param timeoutMillis 超时时长
     * @throws InterruptedException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    public void invokeOnewayImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        // 设别标记， 对端 检查标记， 就可以知道这个请求，是单向请求
        request.markOnewayRPC();
        // 获取信号量，同上
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            // 释放信号量逻辑封装对象。
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
            try {
                // 1. 将数据 交给 channel, 这里数据发送的逻辑，是由netty线程完成的。
                // 2. 添加 写 刷 数据操作的 监听器 由netty IO 线程回调。

                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        // 请求发送后，直接释放
                        once.release();
                        if (!f.isSuccess()) {
                            log.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                        }
                    }
                });
            } catch (Exception e) {
                once.release();
                log.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
            } else {
                String info = String.format(
                        "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                        timeoutMillis,
                        this.semaphoreOneway.getQueueLength(),
                        this.semaphoreOneway.availablePermits()
                );
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

    class NettyEventExecutor extends ServiceThread {
        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<NettyEvent>();
        private final int maxSize = 10000;

        public void putNettyEvent(final NettyEvent event) {
            if (this.eventQueue.size() <= maxSize) {
                this.eventQueue.add(event);
            } else {
                log.warn("event queue size[{}] enough, so drop this event {}", this.eventQueue.size(), event.toString());
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

            while (!this.isStopped()) {
                try {
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                            case IDLE:
                                listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CONNECT:
                                listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
                                break;
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(), event.getChannel());
                                break;
                            default:
                                break;

                        }
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return NettyEventExecutor.class.getSimpleName();
        }
    }
}
