package com.hmdp.utils;

import com.sun.corba.se.impl.oa.poa.ActiveObjectMap;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock {
    private String name;
    private StringRedisTemplate redisTemplate;
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    //使用静态代码块，在开始前就已经加载好了
    static{
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    public SimpleRedisLock(String name, StringRedisTemplate redisTemplate) {
        this.name = name;
        this.redisTemplate = redisTemplate;
    }

    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString() + "-";

    @Override
    public boolean tryLock(Long timeoutSec) {
        long threadId = Thread.currentThread().getId();
        Boolean success = redisTemplate.opsForValue()
                .setIfAbsent(KEY_PREFIX + name,threadId + "",timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unlock() {
        //调用lua脚本
        redisTemplate.execute(UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX + name),
                ID_PREFIX + Thread.currentThread().getId());

//        String threadId = ID_PREFIX + Thread.currentThread().getId();
//        String id = redisTemplate.opsForValue().get(KEY_PREFIX + name);
//        if(threadId.equals(id)){
//            redisTemplate.delete(KEY_PREFIX + name);
//        }

    }
}
