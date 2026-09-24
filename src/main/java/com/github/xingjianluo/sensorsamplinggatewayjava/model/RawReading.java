package com.github.xingjianluo.sensorsamplinggatewayjava.model;

/**
 * 传感器驱动产生的原始读数。checksum 为基于读数字段计算的校验和，
 * 驱动正常时与 {@link #expectedChecksum} 一致；传输损坏时不一致。
 *
 * @param sensorId    传感器标识
 * @param seq         该传感器读数的单调递增序列号（用于去重与防丢失核对）
 * @param value       原始测量值
 * @param timestampMs 设备侧时间戳（毫秒）
 * @param checksum    读数携带的校验和
 */
public record RawReading(
        String sensorId,
        long seq,
        double value,
        long timestampMs,
        long checksum
) {

    /**
     * 根据读数字段计算期望校验和。采用确定的 FNV-1a 风格 64 位混合，
     * 不依赖对象身份，保证相同字段恒定得到相同校验和。
     */
    public static long expectedChecksum(String sensorId, long seq, double value, long timestampMs) {
        long hash = 0xcbf29ce484222325L;
        hash = mix(hash, sensorId == null ? 0L : sensorId.hashCode());
        hash = mix(hash, seq);
        hash = mix(hash, Double.doubleToRawLongBits(value));
        hash = mix(hash, timestampMs);
        return hash;
    }

    private static long mix(long hash, long part) {
        long h = hash ^ part;
        h *= 0x100000001b3L;
        return h;
    }

    /**
     * 该校验和是否与字段重新计算的结果一致。
     */
    public boolean checksumValid() {
        return checksum == expectedChecksum(sensorId, seq, value, timestampMs);
    }
}
