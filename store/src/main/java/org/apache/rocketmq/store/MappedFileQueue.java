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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class MappedFileQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private static final int DELETE_FILES_BATCH_MAX = 10;

    // mfq 管理的目录 （commitLog: ../store/commitlog  或者 consumerQueue:  ../store/xxx_topic/0）
    private final String storePath;
    // 目录下每个文件大小  （commitLog文件默认 1g . consumeQueue 文件  默认 600w字节）
    private final int mappedFileSize;

    // list 目录下每个mappedFile  都加入这个list
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

    // 创建mappedFile的服务， 内部有自己的线程， 咱们通过向他提交 request , 内部线程处理完成后，会返回给我们结果。 结果就是 mappedFile对象
    private final AllocateMappedFileService allocateMappedFileService;

    // 目录的刷盘位点， （它的值： curMappendFile.fileName+ curMappedFile.wrotePoisition）
    private long flushedWhere = 0;
    private long committedWhere = 0;

    // 当前目录下最后一条msg存储时间
    private volatile long storeTimestamp = 0;

    /**
     * @param storePath                 ../store/commitlog
     * @param mappedFileSize            默认1g
     * @param allocateMappedFileService allocateMappedFileService
     */
    public MappedFileQueue(final String storePath, int mappedFileSize, AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    public void checkSelf() {

        if (!this.mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();

                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}", pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    public MappedFile getMappedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return null;

        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        return (MappedFile) mfs[mfs.length - 1];
    }

    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;

        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();

        for (MappedFile file : this.mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }

        this.deleteExpiredFile(willRemoveFiles);
    }

    void deleteExpiredFile(List<MappedFile> files) {

        if (!files.isEmpty()) {

            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    public boolean load() {
        // 创建目录对象
        File dir = new File(this.storePath);
        // 获取目录下所有的文件
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            // 按照文件名称排序
            Arrays.sort(files);
            for (File file : files) {
                // 大概率不会进入这个分支。
                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length() + " length not matched message store config value, please check it manually");
                    return false;
                }

                try {
                    // 为当前file创建 对应的 mappedFile对象
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);

                    // 设置 wrotePosition 和  flushedPoistion 这里的值 都是 mappedFileSize ， 并不是准确的。准确值，需要 recover阶段设置。
                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
                    mappedFile.setCommittedPosition(this.mappedFileSize);

                    // 将当前file的 mappedFile 对象 加入到list集合管理。
                    this.mappedFiles.add(mappedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    /**
     * @param startOffset 文件起始偏移量
     * @param needCreate  当list为空时， 是否创建 mappedFile
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        // 该值控制是否创建mappedFile , 当需要创建mappedFile 时， 它充当文件名的结尾
        // 这两种情况会创建
        // 1 list 内没有mappedFile
        // 2 list 最后一个mappedFile (当前顺序写的mappedFile) 它写满了
        long createOffset = -1;
        MappedFile mappedFileLast = getLastMappedFile();

        // 情况1 list内没有mappedFile
        if (mappedFileLast == null) {
            // crateOffset 取值 必须是 mappedFileSize 的倍数 或者0
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }

        // 情况2 list最后一个mappedfile (当前顺序写的mappedFile) 它写满了
        if (mappedFileLast != null && mappedFileLast.isFull()) {
            // 上一个文件名 转long 加上 mappedFileSize
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        // 这里是创建新的mappedFile逻辑
        if (createOffset != -1 && needCreate) {

            // 获取待创建文件的绝对路径。 （下次即将要创建的文件名）
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);

            // 获取 “下下次” 要创建的文件的绝对路径
            String nextNextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
            MappedFile mappedFile = null;


            // 使用 allocateMappedFileService  创建
            if (this.allocateMappedFileService != null) {
                // 创建mappedFile的服务， 内部有自己的线程， 咱们通过向他提交reques.
                // 内部线程处理完成后， 返回的结果就是 mappedFile 对象
                mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath, nextNextFilePath, this.mappedFileSize);
            } else {
                try {
                    mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                } catch (IOException e) {
                    log.error("create mappedFile exception", e);
                }
            }

            if (mappedFile != null) {
                if (this.mappedFiles.isEmpty()) {
                    mappedFile.setFirstCreateInQueue(true);
                }
                this.mappedFiles.add(mappedFile);
            }

            return mappedFile;
        }

        return mappedFileLast;
    }

    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;

        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }

        return mappedFileLast;
    }

    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getFileFromOffset() + mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;

            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff)
                return false;
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();

        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }

    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }


    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }

    /**
     * commitlog 目录删除过期文件调用
     *
     * @param expiredTime         过期时间
     * @param deleteFilesInterval 删除两个文件之间的时间间隔
     * @param intervalForcibly    mf.destory传递的参数
     * @param cleanImmediately    true  强制删除 不考虑expiredTime 这个条件了
     * @return
     */
    public int deleteExpiredFileByTime(final long expiredTime, final int deleteFilesInterval, final long intervalForcibly, final boolean cleanImmediately) {
        // 获取 mfq 数组
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return 0;

        int mfsLength = mfs.length - 1;

        // 删除的文件数
        int deleteCount = 0;

        // 被删除的文件集合
        List<MappedFile> files = new ArrayList<MappedFile>();
        if (null != mfs) {
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];

                // 计算出当前文件的 存活时间截止点
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;

                // 条件一成立、 文件存活时间 达到上限
                // 条件二成立、 disk 占用率达到上限 。 会走强制删除
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    if (mappedFile.destroy(intervalForcibly)) {
                        files.add(mappedFile);
                        deleteCount++;

                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
                            break;
                        }

                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    //avoid deleting files in the middle
                    break;
                }
            }
        }
        // 将删除文件的 mf从queue 中删除
        deleteExpiredFile(files);

        // 返回删除文件数
        return deleteCount;
    }

    /**
     * consumerQueue 目录删除过期文件调用
     *
     * @param offset   commitlog 目录下 最小的物理偏移量
     * @param unitSize consumerQueue 文件内每个数据单元固定大小
     * @return
     */
    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {
            // 这里的 -1 是保证 当前正在顺序写的 mf 不被删除
            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                // 当前mf是否删除
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                // 获取当前文件 最后一个数据单元
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    // 获取  cqData.msgPhyOffset
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    // 释放
                    result.release();
                    // true 说明 当前mf内全部的cqDta 都是过期数据
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset " + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }
        //将删除文件的mf从queue内删除
        deleteExpiredFile(files);
        // 返回删除的文件数
        return deleteCount;
    }

    /**
     * @param flushLeastPages 0 表示强制刷新， >0 脏页数据必须达到flushLeastPages 才刷新。
     * @return boolean true 表示本次刷盘无数据落盘
     */
    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        // 获取当前正在刷盘的文件（ 正在顺序写的mappedFile）
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            // 获取mappedFile 最后一条消息的存储时间
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            // 调用mf刷盘 ， 返回mf最新的落盘位点
            int offset = mappedFile.flush(flushLeastPages);
            // mf起始偏移量 + mf最新的落盘位点
            long where = mappedFile.getFileFromOffset() + offset;
            // true 表示本次刷盘无数据落盘 。 false 表示本次刷盘 有数据落盘
            result = where == this.flushedWhere;
            // 将最新的目录刷盘位点，赋值给 flushedWhere
            this.flushedWhere = where;
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            int offset = mappedFile.commit(commitLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.committedWhere;
            this.committedWhere = where;
        }

        return result;
    }

    /**
     * Finds a mapped file by offset.
     *
     * @param offset                Offset.
     * @param returnFirstOnNotFound If the mapped file is not found, then return the first one.
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            // 第一个mappedFile
            MappedFile firstMappedFile = this.getFirstMappedFile();
            // 最后一个mappedFile
            MappedFile lastMappedFile = this.getLastMappedFile();
            if (firstMappedFile != null && lastMappedFile != null) {
                // 条件成立，说明offset 没能命中 list中的mappedFile
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}", offset, firstMappedFile.getFileFromOffset(), lastMappedFile.getFileFromOffset() + this.mappedFileSize, this.mappedFileSize, this.mappedFiles.size());
                } else {
                    // 正常的逻辑

                    // 根据offset 算在那个mappedFile。 （下标）
                    // 比如说 commitLog目录下有：5g 6g 7g 8g 9g 10g
                    // 我们要查找offset：包含7.6g的mappedFile
                    // (offset / this.mappedFileSize) => 7.6g / 1g => 7
                    // firstMappedFile.getFileFromOffset() / this.mappedFileSize => 5g / 1g => 5
                    // index => 7-5 => 2
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }
                    // 正常在这里返回， 条件成立， 说明mappedFile 内包含offset偏移量
                    if (targetFile != null && offset >= targetFile.getFileFromOffset() && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }

                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset() && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }

                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}
