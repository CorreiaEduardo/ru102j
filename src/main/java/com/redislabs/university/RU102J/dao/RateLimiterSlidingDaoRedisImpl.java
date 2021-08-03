package com.redislabs.university.RU102J.dao;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.Random;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        // START CHALLENGE #7
        try (Jedis jedis = jedisPool.getResource()) {
            final Transaction transaction = jedis.multi();

            final String key = RedisSchema.getSlidingWindowRateLimiterKey(windowSizeMS, name, maxHits);
            final long currentTimeMillis = System.currentTimeMillis();
            final String member = String.format("%d-%d", currentTimeMillis, new Random().nextLong());

            transaction.zadd(key, currentTimeMillis, member);
            transaction.zremrangeByScore(key, 0, currentTimeMillis - windowSizeMS);
            final Response<Long> zcard = transaction.zcard(key);

            transaction.exec();
            transaction.close();

            if (zcard.get() > maxHits) throw new RateLimitExceededException();
        }
        // END CHALLENGE #7
    }
}
