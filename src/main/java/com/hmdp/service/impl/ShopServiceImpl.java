package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate redisTemplate;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {
        Shop shop = queryWithMutex(id);
        if(shop == null){
            return Result.fail("店铺不存在！");
        }
        //返回
        return Result.ok(shop);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺id不能为空");
        }
        //更新数据库
        updateById(shop);

        //删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    //加锁
    private boolean tryLock(String key){
        Boolean  flag = stringRedisTemplate.opsForValue().setIfAbsent(key,"1",10,TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    //释放锁
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    //缓存穿透问题
    public Shop queryWithPassThrough(Long id){
        String key = CACHE_SHOP_KEY + id;
        //从redis查店铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //判断是否存在（null）
        if(StrUtil.isNotBlank(shopJson)){
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        //看是否命中空值
        if(shopJson == null){
            return null;
        }

        //不存在，根据id查数据库
        Shop shop = getById(id);

        //看是否存在
        if(shop != null){
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }

        //存在，打入redis,设置超时时间
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //返回
        return shop;
    }

    private Shop queryWithMutex(Long id){
        String key = CACHE_SHOP_KEY + id;
        //从redis查店铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //判断是否存在（null）
        if(StrUtil.isNotBlank(shopJson)){
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        //看是否命中空值
        if(shopJson == null){
            return null;
        }

        //获得互斥锁
        String lockKey = "lock:shop:" + id;
        boolean isLock = tryLock(lockKey);
        Shop shop = null;
        try {
            if(!isLock){
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //成功拿到锁，再查redis，看有没有这个数据
            shop = getById(id);

            //看是否存在
            if(shop != null){
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }

            //存在，打入redis,设置超时时间
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //释放互斥锁
            unlock(key);
        }

        //返回
        return shop;
    }
}
