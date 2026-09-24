package com.github.xingjianluo.sensorsamplinggatewayjava.reliability;

/** 可替换的睡眠操作：测试可注入空实现，避免真实等待。 */
@FunctionalInterface
public interface Sleeper {
    void sleep(long millis) throws InterruptedException;

    Sleeper REAL = millis -> Thread.sleep(millis);
    Sleeper NOOP = millis -> { };
}
