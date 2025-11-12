package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;

public class CacheClient {
    private final StringRedisTemplate redisTemplate;

    //开线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    //加锁
    private boolean tryLock(String key){
        Boolean flag = redisTemplate.opsForValue().setIfAbsent(key,"1",10,TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    //释放锁
    private void unlock(String key){
        redisTemplate.delete(key);
    }

    public CacheClient(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    //互斥锁方式设置redis
    public void set(String key, Object value,Long time, TimeUnit timeUnit) {
        redisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, timeUnit);
    }

    //设置逻辑过期
    public void setLogicalExpire(String key, Object value,Long time, TimeUnit timeUnit) {
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));
        //写入redis
        redisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }

    //缓存穿透解决方法
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time, TimeUnit timeUnit) {
        String key = keyPrefix + id;
        //从redis查商铺缓存
        String json = redisTemplate.opsForValue().get(key);
        //判断是否存在
        if(StrUtil.isNotBlank(json)){
            return JSONUtil.toBean(json,type);
        }
        //判断命中是否为空
        if(json != null){
            return null;
        }

        //不存在，根据id查数据库
        R r = dbFallback.apply(id);

        //不存在，返回结果
        if(r == null) {
            redisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.SECONDS);
            return null;
        }
        //存在，写进redis
        this.set(key, r, time, timeUnit);

        return r;
    }

    //解决缓存击穿
    public <R,ID> R queryWithLogicalExpire(String keyPrefix,ID id,Class<R> type, Function<ID,R> dbFallback,Long time, TimeUnit timeUnit) {
        String key = keyPrefix + id;
        //从redis查询商铺缓存
        String json = redisTemplate.opsForValue().get(key);
        //判断是否存在
        if(StrUtil.isBlank(json)){
            //不存在，直接null
            return null;
        }
        //存在，要返回json，json要转化为对象，要反序列化
        RedisData redisData = JSONUtil.toBean(json,RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //判断有没有过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            //没有过期，直接返回redis中的结果
            return r;
        }
        //过期，要缓存重建
        String lockKey = LOCK_SHOP_KEY + id;
        //拿到锁，使用redis客户端的setIfAbsent方法来实现类似锁机制
        //setIfAbsent其实就是setnx命令，有key就不给入，无key就会创建，实际上这个key就是个锁标识
        //setnx命令是原子性操作，不会被其他线程打断，同时设置了10秒TTL
        boolean isLock = tryLock(lockKey);
        if(isLock){
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    //查数据库
                    R r1 = dbFallback.apply(id);
                    //存redis
                    this.setLogicalExpire(key, r1, time, timeUnit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(key);
                }
            });
        }
        return r;
    }
}
