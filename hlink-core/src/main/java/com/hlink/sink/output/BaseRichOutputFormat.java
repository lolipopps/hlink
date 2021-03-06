/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hlink.sink.output;


import com.hlink.helper.FormatStateHelper;
import com.hlink.conf.FlinkCommonConf;
import com.hlink.constants.Metrics;
import com.hlink.converter.AbstractRowConverter;
import com.hlink.exception.WriteRecordException;
import com.hlink.manager.DirtyDataManager;
import com.hlink.metrics.AccumulatorCollector;
import com.hlink.metrics.BaseMetric;
import com.hlink.sink.ErrorLimiter;
import com.hlink.sink.WriteErrorTypes;
import com.hlink.utils.DTThreadFactoryUtil;
import com.hlink.utils.ExceptionUtil;
import com.hlink.utils.JsonUtil;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract Specification for all the OutputFormat defined in flinkx plugins
 *
 * <p>Company: www.dtstack.com
 *
 * <p>NOTE Four situations for checkpoint(cp):
 * 1).Turn off cp, batch and timing directly submitted to the database
 *
 * 2).Turn on cp and in AT_LEAST_ONCE model, batch and timing directly commit to the db .
 *    snapshotState???notifyCheckpointComplete???notifyCheckpointAborted Does not interact with the db
 *
 * 3).Turn on cp and in EXACTLY_ONCE model, batch and timing pre commit to the db .
 *    snapshotState pre commit???notifyCheckpointComplete real commit???notifyCheckpointAborted rollback
 *
 * 4).Turn on cp and in EXACTLY_ONCE model, when cp time out snapshotState???notifyCheckpointComplete may never call,
 *    Only call notifyCheckpointAborted.this maybe a problem ,should make users perceive
 *
 *
 * @author huyifan.zju@163.com
 */
public abstract class BaseRichOutputFormat extends RichOutputFormat<RowData> implements CleanupWhenUnsuccessful, InitializeOnMaster, FinalizeOnMaster {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    public static final int LOG_PRINT_INTERNAL = 2000;

    /** ??????????????? */
    protected StreamingRuntimeContext context;
    /** ???????????????checkpoint */
    protected boolean checkpointEnabled;

    /** ???????????? */
    protected String jobName = "defaultJobName";
    /** ??????id */
    protected String jobId;
    /** ????????????id */
    protected int taskNumber;
    /** ??????????????? */
    protected int numTasks;
    /** ??????????????????, openInputFormat()???????????? */
    protected long startTime;

    protected String formatId;
    /** checkpoint????????????map */
    protected FormatStateHelper formatState;

    /**
     * ????????????cp???????????????????????????????????????????????????????????????
     * EXACTLY_ONCE???????????????????????????????????????
     * AT_LEAST_ONCE????????????????????????????????????????????????????????????
     */
    protected CheckpointingMode checkpointMode;
    /** ???????????????????????? */
    protected transient ScheduledExecutorService scheduler;
    /** ???????????????????????????????????? */
    protected transient ScheduledFuture scheduledFuture;
    /** ??????????????????????????????????????????????????? */
    protected long flushIntervalMills;
    /** ?????????????????? */
    protected FlinkCommonConf config;
    /** BaseRichOutputFormat???????????? */
    protected transient volatile boolean closed = false;
    /** ?????????????????? */
    protected int batchSize = 1;
    /** ????????????????????? */
    protected RowData lastRow = null;

    /** ????????????????????????????????? */
    protected transient List<RowData> rows;
    /** ????????????????????? */
    protected AbstractRowConverter rowConverter;
    /** ?????????????????????????????????????????????????????????hive????????????????????????false */
    protected boolean initAccumulatorAndDirty = true;
    /** ?????????????????? */
    protected DirtyDataManager dirtyDataManager;
    /** ?????????????????? */
    protected ErrorLimiter errorLimiter;
    /** ??????????????? */
    protected transient BaseMetric outputMetric;
    /** cp???flush???????????? */
    protected transient AtomicBoolean flushEnable;
    /** ????????????????????? */
    protected long rowsOfCurrentTransaction;

    /** A collection of field names filled in user scripts with constants removed */
    protected List<String> columnNameList = new ArrayList<>();
    /** A collection of field types filled in user scripts with constants removed */
    protected List<String> columnTypeList = new ArrayList<>();

    /** ?????????????????? */
    protected AccumulatorCollector accumulatorCollector;
    protected LongCounter bytesWriteCounter;
    protected LongCounter durationCounter;
    protected LongCounter numWriteCounter;
    protected LongCounter snapshotWriteCounter;
    protected LongCounter errCounter;
    protected LongCounter nullErrCounter;
    protected LongCounter duplicateErrCounter;
    protected LongCounter conversionErrCounter;
    protected LongCounter otherErrCounter;

    @Override
    public void initializeGlobal(int parallelism) {
        //???????????????????????????configure????????????
    }

    @Override
    public void configure(Configuration parameters) {
        // do nothing
    }

    @Override
    public void finalizeGlobal(int parallelism) {
        //????????????????????????
    }

    /**
     * ?????????????????????????????????????????????
     *
     * @param taskNumber ????????????id
     * @param numTasks ???????????????
     *
     * @throws IOException
     */
    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.taskNumber = taskNumber;
        this.numTasks = numTasks;
        this.context = (StreamingRuntimeContext) getRuntimeContext();
        this.checkpointEnabled = context.isCheckpointingEnabled();
        this.batchSize = config.getBatchSize();
        this.rows = new ArrayList<>(batchSize);
        this.flushIntervalMills = config.getFlushIntervalMills();
        this.flushEnable = new AtomicBoolean(true);

        checkpointMode =
                context.getCheckpointMode() == null
                        ? CheckpointingMode.AT_LEAST_ONCE
                        : context.getCheckpointMode();

        Map<String, String> vars = context.getMetricGroup().getAllVariables();
        if(vars != null){
            jobName = vars.getOrDefault(Metrics.JOB_NAME, "defaultJobName");
            jobId = vars.get(Metrics.JOB_ID);
        }

        initStatisticsAccumulator();
        initRestoreInfo();
        initTimingSubmitTask();

        if (initAccumulatorAndDirty) {
            initAccumulatorCollector();
            initErrorLimiter();
            initDirtyDataManager();
        }
        openInternal(taskNumber, numTasks);
        this.startTime = System.currentTimeMillis();

        LOG.info(
                "[{}] open successfully, \ncheckpointMode = {}, \ncheckpointEnabled = {}, \nflushIntervalMills = {}, \nbatchSize = {}, \n[{}]: \n{} ",
                this.getClass().getSimpleName(),
                checkpointMode,
                checkpointEnabled,
                flushIntervalMills,
                batchSize,
                config.getClass().getSimpleName(),
                JsonUtil.toPrintJson(config));
    }

    @Override
    public synchronized void writeRecord(RowData rowData) {
        int size = 0;
        if (batchSize <= 1) {
            writeSingleRecord(rowData);
            size = 1;
        } else {
            rows.add(rowData);
            if (rows.size() >= batchSize) {
                writeRecordInternal();
                size = batchSize;
            }
        }

        updateDuration();
        numWriteCounter.add(size);
        bytesWriteCounter.add(ObjectSizeCalculator.getObjectSize(rowData));
        if(checkpointEnabled){
            snapshotWriteCounter.add(size);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        LOG.info("taskNumber[{}] close()", taskNumber);

        if (closed) {
            return;
        }

        Exception closeException = null;

        // when exist data
        int size = rows.size();
        if (size != 0) {
            try {
                writeRecordInternal();
                numWriteCounter.add(size);
            } catch (Exception e) {
                closeException = e;
            }
        }

        if (this.scheduledFuture != null) {
            scheduledFuture.cancel(false);
            this.scheduler.shutdown();
        }

        try {
            closeInternal();
        } catch (Exception e) {
            LOG.warn("closeInternal() Exception:{}", ExceptionUtil.getErrorMessage(e));
        }

        updateDuration();

        if (outputMetric != null) {
            outputMetric.waitForReportMetrics();
        }

        if (dirtyDataManager != null) {
            try {
                dirtyDataManager.close();
            } catch (Exception e) {
                LOG.error("dirtyDataManager.close() Exception:{}", ExceptionUtil.getErrorMessage(e));
            }
        }

        if (errorLimiter != null) {
            try {
                errorLimiter.updateErrorInfo();
            } catch (Exception e) {
                LOG.warn("errorLimiter.updateErrorInfo() Exception:{}", ExceptionUtil.getErrorMessage(e));
            }

            try {
                errorLimiter.checkErrorLimit();
            } catch (Exception e) {
                LOG.error("errorLimiter.checkErrorLimit() Exception:{}", ExceptionUtil.getErrorMessage(e));
                if (closeException != null) {
                    closeException.addSuppressed(e);
                } else {
                    closeException = e;
                }
            } finally {
                if (accumulatorCollector != null) {
                    accumulatorCollector.close();
                    accumulatorCollector = null;
                }
            }
        }

        if (accumulatorCollector != null) {
            accumulatorCollector.close();
        }

        if (closeException != null) {
            throw new RuntimeException(closeException);
        }

        LOG.info("subtask[{}}] close() finished", taskNumber);
        this.closed = true;
    }

    @Override
    public void tryCleanupOnError() throws Exception {}

    /**
     * ????????????????????????
     */
    protected void initStatisticsAccumulator() {
        errCounter = context.getLongCounter(Metrics.NUM_ERRORS);
        nullErrCounter = context.getLongCounter(Metrics.NUM_NULL_ERRORS);
        duplicateErrCounter = context.getLongCounter(Metrics.NUM_DUPLICATE_ERRORS);
        conversionErrCounter = context.getLongCounter(Metrics.NUM_CONVERSION_ERRORS);
        otherErrCounter = context.getLongCounter(Metrics.NUM_OTHER_ERRORS);
        numWriteCounter = context.getLongCounter(Metrics.NUM_WRITES);
        snapshotWriteCounter = context.getLongCounter(Metrics.SNAPSHOT_WRITES);
        bytesWriteCounter = context.getLongCounter(Metrics.WRITE_BYTES);
        durationCounter = context.getLongCounter(Metrics.WRITE_DURATION);

        outputMetric = new BaseMetric(context);
        outputMetric.addMetric(Metrics.NUM_ERRORS, errCounter);
        outputMetric.addMetric(Metrics.NUM_NULL_ERRORS, nullErrCounter);
        outputMetric.addMetric(Metrics.NUM_DUPLICATE_ERRORS, duplicateErrCounter);
        outputMetric.addMetric(Metrics.NUM_CONVERSION_ERRORS, conversionErrCounter);
        outputMetric.addMetric(Metrics.NUM_OTHER_ERRORS, otherErrCounter);
        outputMetric.addMetric(Metrics.NUM_WRITES, numWriteCounter, true);
        outputMetric.addMetric(Metrics.SNAPSHOT_WRITES, snapshotWriteCounter);
        outputMetric.addMetric(Metrics.WRITE_BYTES, bytesWriteCounter, true);
        outputMetric.addMetric(Metrics.WRITE_DURATION, durationCounter);
    }

    /**
     * ???????????????????????????
     */
    private void initAccumulatorCollector() {
        accumulatorCollector = new AccumulatorCollector(context, Metrics.METRIC_SINK_LIST);
        accumulatorCollector.start();
    }

    /**
     * ???????????????????????????
     */
    private void initErrorLimiter() {
        if (config.getErrorRecord() >= 0 || config.getErrorPercentage() > 0) {
            Double errorRatio = null;
            if (config.getErrorPercentage() > 0) {
                errorRatio = (double) config.getErrorPercentage();
            }
            errorLimiter = new ErrorLimiter(accumulatorCollector, config.getErrorRecord(), errorRatio);
        }
    }

    /**
     * ???????????????????????????
     */
    private void initDirtyDataManager() {
        if (StringUtils.isNotBlank(config.getDirtyDataPath())) {
            dirtyDataManager = new DirtyDataManager(
                    config.getDirtyDataPath(),
                    config.getDirtyDataHadoopConf(),
                    config.getFieldNameList().toArray(new String[0]),
                    jobId);
            dirtyDataManager.open();
            LOG.info("init dirtyDataManager: {}", this.dirtyDataManager);
        }
    }

    /**
     * ???checkpoint????????????map????????????????????????????????????
     */
    private void initRestoreInfo() {
        if (formatState == null) {
            formatState = new FormatStateHelper(taskNumber, null);
        } else {
            errCounter.add(formatState.getMetricValue(Metrics.NUM_ERRORS));
            nullErrCounter.add(formatState.getMetricValue(Metrics.NUM_NULL_ERRORS));
            duplicateErrCounter.add(formatState.getMetricValue(Metrics.NUM_DUPLICATE_ERRORS));
            conversionErrCounter.add(formatState.getMetricValue(Metrics.NUM_CONVERSION_ERRORS));
            otherErrCounter.add(formatState.getMetricValue(Metrics.NUM_OTHER_ERRORS));

            numWriteCounter.add(formatState.getMetricValue(Metrics.NUM_WRITES));

            snapshotWriteCounter.add(formatState.getMetricValue(Metrics.SNAPSHOT_WRITES));
            bytesWriteCounter.add(formatState.getMetricValue(Metrics.WRITE_BYTES));
            durationCounter.add(formatState.getMetricValue(Metrics.WRITE_DURATION));
        }
    }

    /**
     * Turn on timed submission,Each result table is opened separately
     */
    private void initTimingSubmitTask() {
        if (batchSize > 1 && flushIntervalMills > 0) {
            LOG.info("initTimingSubmitTask() ,initialDelay:{}, delay:{}, MILLISECONDS", flushIntervalMills, flushIntervalMills);
            this.scheduler = new ScheduledThreadPoolExecutor(1, new DTThreadFactoryUtil("timer-data-write-thread"));
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
                synchronized (BaseRichOutputFormat.this) {
                    if (closed) {
                        return;
                    }
                    try {
                        if(!rows.isEmpty()){
                            int size = rows.size();
                            writeRecordInternal();
                            numWriteCounter.add(size);
                        }
                    } catch (Exception e) {
                        LOG.error("Writing records failed. {}", ExceptionUtil.getErrorMessage(e));
                    }
                }
            }, flushIntervalMills, flushIntervalMills, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * ??????????????????
     * @param rowData ????????????
     */
    protected void writeSingleRecord(RowData rowData) {
        if (errorLimiter != null) {
            errorLimiter.checkErrorLimit();
        }

        try {
            writeSingleRecordInternal(rowData);
        } catch (WriteRecordException e) {
            // todo ???????????????
            updateDirtyDataMsg(rowData, e);
            if (LOG.isTraceEnabled()) {
                LOG.trace("write error rowData, rowData = {}, e = {}", rowData.toString(), ExceptionUtil.getErrorMessage(e));
            }
        }
    }

    /**
     * ??????????????????
     */
    protected synchronized void writeRecordInternal() {
        if(flushEnable.get()){
            try {
                writeMultipleRecordsInternal();
            } catch (Exception e) {
                //??????????????????????????????
                rows.forEach(this::writeSingleRecord);
            } finally {
                // Data is either recorded dirty data or written normally
                rows.clear();
            }
        }
    }

    /**
     * ?????????????????????
     * @param rowData ?????????????????????
     * @param e ??????
     */
    private void updateDirtyDataMsg(RowData rowData, WriteRecordException e) {
        errCounter.add(1);

        String errMsg = ExceptionUtil.getErrorMessage(e);
        int pos = e.getColIndex();
        if (pos != -1) {
            errMsg += recordConvertDetailErrorMessage(pos, e.getRowData());
        }

        //???2000????????????????????????
        if (errCounter.getLocalValue() % LOG_PRINT_INTERNAL == 0) {
            LOG.error(errMsg);
        }

        if (errorLimiter != null) {
            errorLimiter.setErrMsg(errMsg);
            errorLimiter.setErrorData(rowData);
        }

        if (dirtyDataManager != null) {
            String errorType = dirtyDataManager.writeData(rowData, e);
            switch (errorType){
                case WriteErrorTypes.ERR_NULL_POINTER:
                    nullErrCounter.add(1);
                    break;
                case WriteErrorTypes.ERR_FORMAT_TRANSFORM:
                    conversionErrCounter.add(1);
                    break;
                case WriteErrorTypes.ERR_PRIMARY_CONFLICT:
                    duplicateErrCounter.add(1);
                    break;
                default:
                    otherErrCounter.add(1);
            }
        }
    }

    /**
     * ???????????????????????????
     * @param pos ??????????????????
     * @param rowData ?????????????????????
     * @return ???????????????????????????
     */
    protected String recordConvertDetailErrorMessage(int pos, Object rowData) {
        return String.format("%s WriteRecord error: when converting field[%s] in Row(%s)", getClass().getName(), pos, rowData);
    }

    /**
     * ??????????????????????????????
     */
    protected void updateDuration() {
        if (durationCounter != null) {
            durationCounter.resetLocal();
            durationCounter.add(System.currentTimeMillis() - startTime);
        }
    }

    /**
     * ??????checkpoint????????????map
     * @return
     */
    public synchronized FormatStateHelper getFormatState() throws Exception {
        // not EXACTLY_ONCE model,Does not interact with the db
        if (CheckpointingMode.EXACTLY_ONCE == checkpointMode) {
            try {
                LOG.info("getFormatState:Start preCommit, rowsOfCurrentTransaction: {}", rowsOfCurrentTransaction);
                preCommit();
            } catch (Exception e) {
                LOG.error("preCommit error, e = {}", ExceptionUtil.getErrorMessage(e));
            } finally {
                flushEnable.compareAndSet(true, false);
            }
        }
        //set metric after preCommit
        formatState.setNumberWrite(numWriteCounter.getLocalValue());
        formatState.setMetric(outputMetric.getMetricCounters());
        LOG.info("format state:{}", formatState.getState());
        return formatState;
    }

    /**
     * pre commit data
     * @throws Exception
     */
    protected void preCommit() throws Exception{}

    /**
     * ??????????????????
     *
     * @param rowData ??????
     *
     * @throws WriteRecordException
     */
    protected abstract void writeSingleRecordInternal(RowData rowData) throws WriteRecordException;

    /**
     * ??????????????????
     *
     * @throws Exception
     */
    protected abstract void writeMultipleRecordsInternal() throws Exception;

    /**
     * ???????????????????????????
     *
     * @param taskNumber ????????????
     * @param numTasks ????????????
     *
     * @throws IOException
     */
    protected abstract void openInternal(int taskNumber, int numTasks) throws IOException;

    /**
     * ???????????????????????????
     *
     * @throws IOException
     */
    protected abstract void closeInternal() throws IOException;

    /**
     * checkpoint???????????????
     *
     * @param checkpointId
     */
    public synchronized void notifyCheckpointComplete(long checkpointId) {
        if (CheckpointingMode.EXACTLY_ONCE == checkpointMode) {
            try {
                commit(checkpointId);
                LOG.info("notifyCheckpointComplete:Commit success , checkpointId:{}", checkpointId);
            } catch (Exception e) {
                LOG.error("commit error, e = {}", ExceptionUtil.getErrorMessage(e));
            } finally {
                flushEnable.compareAndSet(false, true);
            }
        }
    }

    /**
     * commit data
     * @param checkpointId
     * @throws Exception
     */
    public void commit(long checkpointId) throws Exception{}

    /**
     * checkpoint???????????????
     *
     * @param checkpointId
     */
    public synchronized void notifyCheckpointAborted(long checkpointId) {
        if (CheckpointingMode.EXACTLY_ONCE == checkpointMode) {
            try{
                rollback(checkpointId);
                LOG.info("notifyCheckpointAborted:rollback success , checkpointId:{}", checkpointId);
            } catch (Exception e) {
                LOG.error("rollback error, e = {}", ExceptionUtil.getErrorMessage(e));
            } finally{
                flushEnable.compareAndSet(false, true);
            }
        }
    }

    /**
     * rollback data
     * @param checkpointId
     * @throws Exception
     */
    public void rollback(long checkpointId) throws Exception{}


    public void setRestoreState(FormatStateHelper formatState) {
        this.formatState = formatState;
    }

    public String getFormatId() {
        return formatId;
    }

    public void setFormatId(String formatId) {
        this.formatId = formatId;
    }

    public void setDirtyDataManager(DirtyDataManager dirtyDataManager) {
        this.dirtyDataManager = dirtyDataManager;
    }

    public void setErrorLimiter(ErrorLimiter errorLimiter) {
        this.errorLimiter = errorLimiter;
    }

    public FlinkCommonConf getConfig() {
        return config;
    }

    public void setConfig(FlinkCommonConf config) {
        this.config = config;
    }

    public void setRowConverter(AbstractRowConverter rowConverter) {
        this.rowConverter = rowConverter;
    }
}
