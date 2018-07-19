package com.clouway.tsbonev

import com.google.appengine.api.memcache.MemcacheServiceFactory
import com.google.appengine.api.memcache.stdimpl.GCacheFactory
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig
import com.google.appengine.tools.development.testing.LocalServiceTestHelper
import org.hamcrest.CoreMatchers.`is` as Is
import org.junit.After
import org.junit.Assert.assertThat
import org.junit.Before
import org.junit.Test
import java.util.concurrent.TimeUnit

class LocalMemcacheTest {

    private val helper = LocalServiceTestHelper(LocalMemcacheServiceTestConfig())

    @Before
    fun setUp() {
        helper.setUp()
    }

    @After
    fun tearDown() {
        helper.tearDown()
    }

    @Test
    fun createMemcache(){

        val mc = MemcacheServiceFactory.getMemcacheService()
        val key = "key"
        val value = byteArrayOf(12)

        mc.put(key, value)

        assertThat(mc.contains(key), Is(true))

    }

    @Test
    fun retireValueFromMemcache(){

        val mc = MemcacheServiceFactory.getMemcacheService()
        val key = "key"
        val value = byteArrayOf(2)

        mc.put(key, value)

        assertThat(mc.contains(key), Is(true))

        mc.delete(key)

        assertThat(mc.contains(key), Is(false))

    }

}