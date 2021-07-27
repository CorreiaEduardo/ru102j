package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.api.Site;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;

public class SiteDaoRedisImpl implements SiteDao {
    private final JedisPool jedisPool;

    public SiteDaoRedisImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    // When we insert a site, we set all of its values into a single hash.
    // We then store the site's id in a set for easy access.
    @Override
    public void insert(Site site) {
        try (Jedis jedis = jedisPool.getResource()) {
            String hashKey = RedisSchema.getSiteHashKey(site.getId());
            String siteIdKey = RedisSchema.getSiteIDsKey();
            jedis.hmset(hashKey, site.toMap());
            jedis.sadd(siteIdKey, hashKey);
        }
    }

    @Override
    public Site findById(long id) {
        String key = RedisSchema.getSiteHashKey(id);
        return parseSite(key);
    }

    // Challenge #1
    @Override
    public Set<Site> findAll() {
        // START Challenge #1
        Set<Site> result = new HashSet<>();
        try (Jedis jedis = jedisPool.getResource()) {
            final Set<String> siteIds = jedis.smembers(RedisSchema.getSiteIDsKey());
            siteIds.forEach(siteId -> {
                final Site site = parseSite(siteId);
                result.add(site);
            });
        }
        return result;
        // END Challenge #1
    }

    private Site parseSite(String key) {
        try(Jedis jedis = jedisPool.getResource()) {
            Map<String, String> fields = jedis.hgetAll(key);
            if (fields == null || fields.isEmpty()) {
                return null;
            } else {
                return new Site(fields);
            }
        }
    }
}
