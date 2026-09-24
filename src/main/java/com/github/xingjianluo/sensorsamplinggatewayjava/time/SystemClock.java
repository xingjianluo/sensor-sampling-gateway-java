package com.github.xingjianluo.sensorsamplinggatewayjava.time;

/** 生产环境时钟：直接读取系统墙钟。 */
public final class SystemClock implements Clock {
    @Override
    public long nowMillis() {
        return System.currentTimeMillis();
    }
}
