package com.hmdp.service.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        //查缓存
        String key = "cache:shopType";

        //查找
        List<String> shopTypeJson = stringRedisTemplate.opsForList().range(key, 0, -1);
        //有就返回
        if(!CollectionUtil.isEmpty(shopTypeJson)){
            //json字符串转list
            List<ShopType> shopTypes = JSONUtil.toList(shopTypeJson.toString(), ShopType.class);
            //排序
            Collections.sort(shopTypes,(((o1, o2) -> o1.getSort() - o2.getSort())));
            return Result.ok(shopTypes);
        }

        //没有就接着查数据库
        List<ShopType> shopTypes = query().orderByAsc("sort").list();

        //不存在，直接返回error
        if(shopTypes == null){
            return Result.fail("不存在商铺类型，查询失败");
        }

        //数据库有，写入redis
        List<String> shopTypesJson = shopTypes.stream().
                map(shopType -> JSONUtil.toJsonStr(shopType)).
                collect(Collectors.toList());

        //从数据库出来已按顺序，这时候需要从右push进去，才能保证原来顺序不变
        stringRedisTemplate.opsForList().rightPushAll(key, shopTypesJson);

        //返回
        return Result.ok(shopTypes);

    }
}
