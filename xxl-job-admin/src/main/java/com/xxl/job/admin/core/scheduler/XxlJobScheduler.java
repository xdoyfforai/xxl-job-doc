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

        // admin trigger pool start(初始化调度线程池)
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

        // admin lose-monitor run ( depend on JobTriggerPoolHelper )(任务完成处理)
        // 启动一个线程每60秒对xxl_job_log表进行扫描，找出所有停留在 "运行中" 状态超过10min，且对应执行器心跳注册失败不在线的job，将其标记为失败
        // 此类还提供了
        JobCompleteHelper.getInstance().start();

        // admin log report start(运行报表统计处理)
        // 启动一个线程每1分钟对xxl_job_log表进行扫描
        //      1：统计处当天及前两天运行job的成功失败情况，更新到xxl_job_log_report表中，以供前端页面使用
        //      2：删除所有处于配置指定的log保存天数以前的log（每一天只会清理一次）
        JobLogReportHelper.getInstance().start();

        // start-schedule  ( depend on JobTriggerPoolHelper )(核心调度处理，使用前面启动的调度线程池：JobTriggerPoolHelper)
        // 启动一个线程每一整数秒执行如下操作（前一次操作时间大于一秒则不等待直接尝试下次调度）：
        //      1：获取分布式锁，成功则继续
        //      2：获取xxl_job_info表中所有已经启动(trigger_status=1)且下次触发时间<当前时间+5s的job信息(取不到则跳过本次调度(释放锁)，到下一个整数5秒继续尝试调度)
        //          2.1：当前job触发时间过期5s以上->根据job的调度过期策略(misfire_strategy)选择忽略本次调度或者立即执行一次->刷新下次调度时间
        //          2.2：当前job触发时间过期5s以内->直接触发一次->刷新下次调度时间（如果下一次调度的时间仍然在下五秒以内则加入时间轮然后再次刷新下次调度时间）
        //          2.3：当前job触发时间在未来5s以内->加入时间轮->刷新下次调度时间
        //      3：更新入库所有本次处理的job的调度信息（上次触发时间，下次触发时间，调度status）
        //      4：释放分布式锁
        //
        // 启动时间轮线程在下一个整数秒后执行如下操作（每一秒执行一次）：
        //      1：在时间轮map中拿到当前秒数以及前一秒数(防止执行时间过长跳刻度)的jobid列表，循环调度所有任务(交由JobTriggerPoolHelper通过线程池调度，提高调度效率)
        JobScheduleHelper.getInstance().start();

        // 单个任务调度的具体逻辑：
        //      1：任务调度由两个线程池完成fastTriggerPool和slowTriggerPool 如果在1min时间窗口内，某jobid对应的job调度花费的时间>500ms超过10次，
        //          则交由slowTriggerPool来调度 避免对fastTriggerPool的调度能力形成拖累
        //      2：具体调度逻辑交由XxlJobTrigger.trigger()来处理,该方法获取此次调度的任务信息jobinfo和执行此任务的执行器集群信息jobgroup，
        //          若路由规则选择'分片广播',则对执行器集群下的所有节点进行任务调度，否则只选择一个节点调度，具体哪一个由路由策略决定
        //      3：执行调度的内部方法XxlJobTrigger.processTrigger()
        //          1: 在log表中插入一条新的数据来记录此次调度
        //          2：生成TriggerParam 包括jobid handler名称 执行参数 阻塞处理策略 logid 分片广播参数等
        //          3：确定调度的执行器地址 对于分片广播 根据方法参数(index,total)以及jobgroup中的addresslist确定，其他路由策略则由不同策略持有
        //              的ExecutorRouter router来确定,router实现类在com.xxl.job.admin.core.route.strategy包下
        //          4: 地址确定后，配合生成的TriggerParam由ExecutorBiz的实现类(ExecutorBizClient)执行远程调用,
        //              ExecutorBizClient通过executorBizRepository缓存在ConcurrentHashMap中<address, ExecutorBizClient>
        //          5: 根据远程调用成功与否，生成XxlJobLog对象 更新log表
        // 至此 调度中心的调度过程结束

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
