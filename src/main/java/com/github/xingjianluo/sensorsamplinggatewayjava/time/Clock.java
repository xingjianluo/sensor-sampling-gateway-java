package com.github.xingjianluo.sensorsamplinggatewayjava.time;

/** 可替换时钟：生产环境为系统时钟，测试环境为手动时钟以保证时间判定可复现。 */
public interface Clock {
    long nowMillis();
}
