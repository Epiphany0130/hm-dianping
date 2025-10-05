package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.controller.ShopController;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;

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
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Object queryById(Long id) {
        // 缓存穿透
//        Shop shop = queryWithPassThrough(id);
//        Shop shop = cacheClient.queryWithPassThrough(
//                id, Shop.class, CACHE_SHOP_KEY, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);

        // 逻辑过期解决缓存穿透
//        Shop shop = queryWithLogicalExpire(id);
        Shop shop = cacheClient.queryWithLogicalExpire(
                id, Shop.class, CACHE_SHOP_KEY, this::getById, 20L, TimeUnit.SECONDS);

        if (shop == null) {
            return Result.fail("店铺不存在");
        }

        // 返回
        return Result.ok(shop);
    }


//    public Shop queryWithMutex(Long id) {
//        String key = CACHE_SHOP_KEY + id;
//        // 1. 从 Redis 中查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2. 判断是否存在
//        if(StrUtil.isNotBlank(shopJson)) {
//            // 3. 存在，直接返回
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
//            return shop;
//        }
//        // 判断命中的是否是空值
//        if(shopJson != null) {
//            // 返回错误信息
//            return null;
//        }
//
//        // 4. 实现缓存重建
//        // 4.1 获取互斥锁
//        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
//        Shop shop = null;
//        try {
//            boolean isLock = tryLock(lockKey);
//            // 4.2 判断是否获取成功
//            if(!isLock) {
//                // 4.3 失败，休眠并重试
//                Thread.sleep(50);
//                return queryWithMutex(id);
//            }
//            // 补：成功检测 redis 是否存在，做 DoubleCheck，这里把 1 2 步拷贝一份
//            // 1. 从 Redis 中查询商铺缓存
//            String shopJsonDouble = stringRedisTemplate.opsForValue().get(key);
//            // 2. 判断是否存在
//            if(StrUtil.isNotBlank(shopJsonDouble)) {
//                // 3. 存在，直接返回
//                Shop shopDouble = JSONUtil.toBean(shopJsonDouble, Shop.class);
//                return shopDouble;
//            }
//            // 判断命中的是否是空值
//            if(shopJsonDouble != null) {
//                // 返回错误信息
//                return null;
//            }
//            // 4.4 成功，根据 id 查询数据库
//            shop = getById(id);
//            // 5. 不存在，返回错误
//            if(shop == null) {
//                // 将空值写入 Redis
//                stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
//
//                // 返回错误信息
//                return null;
//            }
//            // 6. 存在，写入 Redis
//            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        } finally {
//            // 7. 释放互斥锁
//            unlock(lockKey);
//        }
//
//        // 8. 返回
//        return shop;
//    }

//    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

//    public Shop queryWithLogicalExpire(Long id) {
//        String key = CACHE_SHOP_KEY + id;
//        // 1. 从 Redis 中查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2. 判断是否存在
//        if(StrUtil.isBlank(shopJson)) {
//            // 3. 存在，直接返回
//            return null;
//        }
//
//        // 4. 命中，需要先把 json 反序列化为对象
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//
//        // 5. 判断是否过期
//        if(expireTime.isAfter(LocalDateTime.now())) {
//            // 5.1 未过期，直接返回店铺信息
//            return shop;
//        }
//
//        // 5.2 已过期，需要缓存重建
//        // 6. 缓存重建
//        // 6.1 获取互斥锁
//        String lockKey = LOCK_SHOP_KEY + id;
//        boolean isLock = tryLock(lockKey);
//
//        // 6.2 判断是否获取成功
//        if(isLock) {
//            // 6.3 成功，开启独立线程，实现缓存重建
//            CACHE_REBUILD_EXECUTOR.submit(() -> {
//                try {
//                    // 重建缓存
//                    this.saveShop2Redis(id, 20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                } finally {
//                    // 释放锁
//                    unlock(lockKey);
//                }
//            });
//
//        }
//
//
//        // 6.4 失败，返回过期的店铺信息
//        return shop;
//    }

    // 存储店铺信息逻辑过期时间
//    private void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
//        // 1. 查询店铺数据
//        Shop shop = getById(id);
//        Thread.sleep(200); // 模拟重建延时
//
//        // 2. 封装逻辑过期时间
//        RedisData redisData = new RedisData();
//        redisData.setData(shop);
//        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
//
//        // 3. 写入 Redis
//        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
//    }

    public Shop queryWithPassThrough(Long id) {
        String key = CACHE_SHOP_KEY + id;
        // 1. 从 Redis 中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2. 判断是否存在
        if(StrUtil.isNotBlank(shopJson)) {
            // 3. 存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        // 判断命中的是否是空值
        if(shopJson != null) {
            // 返回错误信息
            return null;
        }

        // 4. 不存在，根据 id 查询数据库
        Shop shop = getById(id);
        // 5. 不存在，返回错误
        if(shop == null) {
            // 将空值写入 Redis
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);

            // 返回错误信息
            return null;
        }
        // 6. 存在，写入 Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // 7. 返回
        return shop;
    }

//    // 尝试添加互斥锁
//    private boolean tryLock(String key) {
//        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
//        return BooleanUtil.isTrue(flag);
//    }
//
//    // 释放互斥锁
//    private void unlock(String key) {
//        stringRedisTemplate.delete(key);
//    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null) {
            return Result.fail("店铺id不能为空");
        }
        // 1. 更新数据库
        updateById(shop);
        // 2. 删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
