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
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 引用资源
 */
public abstract class ReferenceResource {

    /**
     * refCount: 引用数量， 当引用数量<= 0 的时候，表示该资源可以释放了，没有其他程序依赖它了、
     * 初始值 1
     */
    protected final AtomicLong refCount = new AtomicLong(1);

    // 是否存活， 默认值为true , 当他为false 时，表示资源处于非存活状态。 不可用
    protected volatile boolean available = true;

    // 是否已经清理，默认值false 当执行完子类对象的cleanup() 后，该值设置为true, 表示资源全部释放了
    protected volatile boolean cleanupOver = false;

    // 第一次关闭资源的时间，（因为第一次关闭资源，可能会失败。 比如说 外部程序 还依赖当前资源 refCount > 0. 此时在这 记录 初次关闭资源的时间）
    // 当之后， 再次关闭资源的时候，会传递一个interval 参数。 如果 系统当前时间 - firstShutdownTimestamp 时间 > interval 。 则强制关闭
    private volatile long firstShutdownTimestamp = 0;

    /**
     * 增加引用计数
     * 返回值 boolean
     *
     * @return
     */
    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    /**
     * 关闭资源
     *
     * @param intervalForcibly 强制关闭资源的时间间隔
     */
    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            this.available = false;
            // 保存初次关闭时的系统时间
            this.firstShutdownTimestamp = System.currentTimeMillis();
            // 引用计数 -1 （有可能释放， 也有可能未释放）
            this.release();
        } else if (this.getRefCount() > 0) {
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                // 强制设置引用计数 为 负数
                this.refCount.set(-1000 - this.getRefCount());
                // 一定会释放资源的
                this.release();
            }
        }
    }

    /**
     *
     */
    public void release() {
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;
        // 执行到这里 说明当前资源 没有其他程序依赖了， 可以调用 cleanup 释放真正的资源了
        synchronized (this) {

            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
