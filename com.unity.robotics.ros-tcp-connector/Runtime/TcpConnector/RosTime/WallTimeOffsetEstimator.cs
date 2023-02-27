//
// Created By: Peter Smith
// Date:       2023-02-27

namespace Unity.Robotics.ROSTCPConnector.RosTime
{
    public class WallTimeOffsetEstimator : ScaledTimeEstimator
    {
        public override float TimeScale => 1.0f;
        public override bool IsPaused => false;
        public override float MaximumDeltaTime => float.MaxValue;
    }
}
