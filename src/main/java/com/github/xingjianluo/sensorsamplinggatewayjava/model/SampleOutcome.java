package com.github.xingjianluo.sensorsamplinggatewayjava.model;

/**
 * 单个原始读数的采样结论：要么被接受为校准样本，要么携带可区分原因被拒绝。
 * sealed 保证结论只有这两种，且拒绝一定带原因与判定依据。
 */
public sealed interface SampleOutcome permits SampleOutcome.Accepted, SampleOutcome.Rejected {

    String sensorId();

    long seq();

    /** 接受：读数通过全部校验并完成校准。 */
    record Accepted(Sample sample) implements SampleOutcome {
        @Override
        public String sensorId() {
            return sample.sensorId();
        }

        @Override
        public long seq() {
            return sample.seq();
        }
    }

    /**
     * 拒绝：读数不得进入采样结果。
     *
     * @param reading 被拒绝的原始读数
     * @param reason  可区分的拒绝原因
     * @param basis   人可读的判定依据（输入值、阈值、规则版本），用于日志与复盘
     */
    record Rejected(RawReading reading, RejectReason reason, String basis) implements SampleOutcome {
        @Override
        public String sensorId() {
            return reading.sensorId();
        }

        @Override
        public long seq() {
            return reading.seq();
        }
    }
}
