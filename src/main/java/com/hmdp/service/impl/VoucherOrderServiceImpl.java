package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct//当前类初始化完毕即执行
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandle());
    }

//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
//    private class VoucherOrderHandle implements Runnable {
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    //获取队列中的订单信息
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    //创建订单
//                    handlerVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单异常", e);
//                    throw new RuntimeException(e);
//                }
//            }
//        }
//    }
    private class VoucherOrderHandle implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    //获取消息队列中的订单信息  XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
                    );
                    //判断消息获取是否成功
                    if(list == null || list.isEmpty()){
                        //如果获取失败，则进入下一次循环
                        continue;
                    }
                    //解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //如果获取成功，则可以下单
                    handlerVoucherOrder(voucherOrder);
                    //ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge("stream.orders", "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

    private void handlePendingList() {
        while (true) {
            try {
                //获取pending-list中的订单信息  XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", "c1"),
                        StreamReadOptions.empty().count(1),
                        StreamOffset.create("stream.orders", ReadOffset.from("0"))
                );
                //判断消息获取是否成功
                if(list == null || list.isEmpty()){
                    //如果获取失败，说明pengding-list中没有异常消息
                    break;
                }
                //解析消息中的订单信息
                MapRecord<String, Object, Object> record = list.get(0);
                Map<Object, Object> values = record.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                //如果获取成功，则可以下单
                handlerVoucherOrder(voucherOrder);
                //ACK确认 SACK stream.orders g1 id
                stringRedisTemplate.opsForStream().acknowledge("stream.orders", "g1", record.getId());
            } catch (Exception e) {
                log.error("处理pending-list异常", e);
            }
        }
    }
}
    private void handlerVoucherOrder(VoucherOrder voucherOrder) {
        //创建锁对象
        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userID, stringRedisTemplate);
        //获取用户
        Long userId = voucherOrder.getUserId();

        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        //获取锁
        if(!isLock){
            log.error("不允许重复下单");
            return ;
        }
        //获取代理对象-事务
        //需要在pom.xml写入aspectj依赖包
        //IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
        try {
            proxy.createVoucherOrder(voucherOrder);
        }finally {
            lock.unlock();
        }
    }


    private IVoucherOrderService proxy;
    @Override
    public Result secKillVoucher(Long voucherId){
        Long userId = UserHolder.getUser().getId();
        long orderID = redisIdWorker.nextId("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderID)
        );
        int r = result.intValue();
        //2.判断是否为0
        if(r != 0) {
            //2.1不为0，没有购买资格
            return Result.fail(r == 1? "库存不足" : "不能重复下单");
        }
        //2.2为0，有购买资格，把下单信息保存到阻塞队列
        //订单ID
//        VoucherOrder voucherOrder = new VoucherOrder();
//        voucherOrder.setId(orderID);
//        //用户ID
//        voucherOrder.setUserId(userId);
//        //优惠券ID
//        voucherOrder.setVoucherId(voucherId);
//        orderTasks.add(voucherOrder);
        //获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //3.返回
        return Result.ok(orderID);
    }

//    @Override
//    public Result secKillVoucher(Long voucherId) {
//        //1.查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //2.判断秒杀是否开始
//        //2.1未开始则返回异常
//        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            return Result.fail("秒杀尚未开始");
//        }
//        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
//            return Result.fail("秒杀已经结束");
//        }
//        //2.2开始则进行下一步查看优惠券库存
//        //3.库存不足返回异常
//        if(voucher.getStock() < 1) {
//            return Result.fail("库存不足");
//        }
//        //上锁
//        Long userID = UserHolder.getUser().getId();
//        //创建锁对象
//        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userID, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userID);
//        boolean isLock = lock.tryLock();
//        //获取锁
//        if(!isLock){
//            return Result.fail("不允许重复下单");
//        }
//            //获取代理对象-事务
//            //需要在pom.xml写入aspectj依赖包
//            //IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//        try {
//            return this.createVoucherOrder(voucherId);
//        }finally {
//            lock.unlock();
//        }
//    }

    @Transactional
    public Result createVoucherOrder(VoucherOrder voucherOrder) {
        //4.1一人一单
        Long userID = UserHolder.getUser().getId();
        int count = query().eq("user_id", userID).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if(count > 0){
            log.error("该用户已经购买过");
            return null;
        }
        //4.2库存足够则扣掉库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                .update();
        if(!success){
            log.error("库存不足");
            return null;
        }
        //5.创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        long orderID = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderID);
//        Long userId = UserHolder.getUser().getId();
//        voucherOrder.setUserId(userId);
//        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);
        return null;
    }
}
