package com.xxl.job.admin.core.scheduler;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.thread.*;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.client.ExecutorBizClient;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author xuxueli 2018-10-28 00:18:17
 */

public class XxlJobScheduler  {
    private static final Logger logger = LoggerFactory.getLogger(XxlJobScheduler.class);


    public void init() throws Exception {
        // init i18n
        initI18n();

        // admin trigger pool start
        JobTriggerPoolHelper.toStart();

        // admin registry monitor run（执行器注册处理）
        // 启动一个线程，每30秒进行执行器相关的注册信息检查
        //      1：检查xxl_job_registry表，如果最后更新时间update_time小于当前时间超过90秒，则认为
        //          该执行器已经下线（执行器需要每30s向调度中心展示心跳，以表示其在线），从表中删除
        //      2：对xxl_job_registry表进行信息统计，将同属于一个appname的执行器地址列表更新到xxl_job_group中

        // 此类同时也提供了手动注册和删除执行器的实现以供前端页面使用
        JobRegistryHelper.getInstance().start();

        // admin fail-monitor run(失败任务处理)
        // 启动一个线程每10秒对xxl_job_log表进行扫描，找出所有失败任务的log，以及对应的job信息，循环处理如下：
        //      1：把job log的alarm status设成-1（锁定状态），如果更新成功，则在当前调度中心进行后续处理，否则表示其他调度中心正在处理，此次放弃
        //      如果log显示这个job的失败重试次数（executor_fail_retry_count）>0，调用JobTriggerPoolHelper.trigger重新调度一次并把本次调度的log加到trigger_msg(调度日志)的后面
        //      2：执行任务失败报警逻辑（此处可定制其他报警方式），更新报警状态：1-无需告警、2-告警成功、3-告警失败
        JobFailMonitorHelper.getInstance().start();

        // admin lose-monitor run ( depend on JobTriggerPoolHelper )
        JobCompleteHelper.getInstance().start();

        // admin log report start
        JobLogReportHelper.getInstance().start();

        // start-schedule  ( depend on JobTriggerPoolHelper )
        JobScheduleHelper.getInstance().start();

        logger.info(">>>>>>>>> init xxl-job admin success.");
    }

    
    public void destroy() throws Exception {

        // stop-schedule
        JobScheduleHelper.getInstance().toStop();

        // admin log report stop
        JobLogReportHelper.getInstance().toStop();

        // admin lose-monitor stop
        JobCompleteHelper.getInstance().toStop();

        // admin fail-monitor stop
        JobFailMonitorHelper.getInstance().toStop();

        // admin registry stop
        JobRegistryHelper.getInstance().toStop();

        // admin trigger pool stop
        JobTriggerPoolHelper.toStop();

    }

    // ---------------------- I18n ----------------------

    private void initI18n(){
        for (ExecutorBlockStrategyEnum item:ExecutorBlockStrategyEnum.values()) {
            item.setTitle(I18nUtil.getString("jobconf_block_".concat(item.name())));
        }
    }

    // ---------------------- executor-client ----------------------
    private static ConcurrentMap<String, ExecutorBiz> executorBizRepository = new ConcurrentHashMap<String, ExecutorBiz>();
    public static ExecutorBiz getExecutorBiz(String address) throws Exception {
        // valid
        if (address==null || address.trim().length()==0) {
            return null;
        }

        // load-cache
        address = address.trim();
        ExecutorBiz executorBiz = executorBizRepository.get(address);
        if (executorBiz != null) {
            return executorBiz;
        }

        // set-cache
        executorBiz = new ExecutorBizClient(address, XxlJobAdminConfig.getAdminConfig().getAccessToken());

        executorBizRepository.put(address, executorBiz);
        return executorBiz;
    }

}
