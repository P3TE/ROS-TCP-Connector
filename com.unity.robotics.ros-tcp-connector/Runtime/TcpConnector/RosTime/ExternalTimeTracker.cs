using System;
using RosMessageTypes.BuiltinInterfaces;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector.RosTime
{
    public class ExternalTimeTracker
    {
        private const double _MaximumSecondsBehindBeforeJumpForward = 0.1f;
        private const double _MaximumSecondsAheadBeforeJumpBackward = 1.0f;
        private const double _MaximumCompensationChangeSecondsPerSecond = 0.025f;

        // When attempting to go back in time, allow time to progress forward but by a smaller amount
        // until it catches up, this is the multiplier of real time used as a minimium
        private const double _SmallestMultipleTimeStep = 0.25f;
        private const double _MinimumCatchupMultiplier = 5f;

        private readonly ScaledTimeEstimator scaledTimeEstimator;

        public bool AnyMessagesReceived
        {
            get;
            private set;
        } = false;

        private TimeMsg lastRecordedTimeScaledTimeEstimate = new TimeMsg(0, 0);

        // Error Compensation:
        private double goalErrorAddedSeconds = 0.0;

        private TimeMsg latestTimeEstimate = new TimeMsg(0, 0);

        public double GoalErrorAddedSeconds => goalErrorAddedSeconds;

        public ExternalTimeTracker(ScaledTimeEstimator scaledTimeEstimator)
        {
            this.scaledTimeEstimator = scaledTimeEstimator;
        }

        public void Reset()
        {
            AnyMessagesReceived = false;
        }

        public void OnNewValueReceived(TimeMsg newTimeValue, bool jumpToNewValue)
        {

            if (!AnyMessagesReceived)
            {
                AnyMessagesReceived = true;
                jumpToNewValue = true;
            }

            TimeMsg currentScaledTimeEstimate = scaledTimeEstimator.UpdateAndGetEstimation();

            // Calculate what we thought the correct time would be.
            TimeMsg ourCurrentEstimate = GetCurrentEstimate(currentScaledTimeEstimate);

            // Using 'newTimeValue' determine how far off we were.
            DurationMsg fromOurEstimateToNewMessage = RosTimeHelper.FromTo(ourCurrentEstimate, newTimeValue);
            double fromOurEstimateToNewMessageSeconds = RosTimeHelper.ToSec(fromOurEstimateToNewMessage);
            if (fromOurEstimateToNewMessageSeconds > _MaximumSecondsBehindBeforeJumpForward)
            {
                // Jump immediately forward to the received value.
                jumpToNewValue = true;
            } else if (fromOurEstimateToNewMessageSeconds < -_MaximumSecondsAheadBeforeJumpBackward)
            {
                Debug.LogWarning($"Jumping back in time! fromOurEstimateToNewMessageSeconds = {fromOurEstimateToNewMessageSeconds}");
                // Jump immediately backward to the received value.
                jumpToNewValue = true;
            } else
            {
                // We aren't too far off, stay with the current estimate and update
                // the direction we are steering.
                latestTimeEstimate = ourCurrentEstimate;
                goalErrorAddedSeconds = fromOurEstimateToNewMessageSeconds;
            }

            if (jumpToNewValue)
            {
                // We are REALLY far off
                // Jump immediately to the received value.
                latestTimeEstimate = newTimeValue;
                goalErrorAddedSeconds = 0.0;
            }
            else
            {
                // We aren't too far off, stay with the current estimate and update
                // the direction we are steering.
                latestTimeEstimate = ourCurrentEstimate;
                goalErrorAddedSeconds = fromOurEstimateToNewMessageSeconds;
            }

            lastRecordedTimeScaledTimeEstimate = currentScaledTimeEstimate;
            AnyMessagesReceived = true;
        }


        public TimeMsg GetCurrentEstimate(TimeMsg scaledTimeEstimate = null)
        {
            if (scaledTimeEstimate == null)
            {
                scaledTimeEstimate = scaledTimeEstimator.UpdateAndGetEstimation();
            }

            //Calculate the time elapsed since the last received message.
            DurationMsg timeSinceLastReceivedValue =
                RosTimeHelper.FromTo(lastRecordedTimeScaledTimeEstimate, scaledTimeEstimate);
            double timeSinceLastReceivedValueSeconds = RosTimeHelper.ToSec(timeSinceLastReceivedValue);

            // Compensate for errors in estimation by moving towards the current goal value.
            double absoluteCompensationChange =
                timeSinceLastReceivedValueSeconds * _MaximumCompensationChangeSecondsPerSecond;
            double compensationChangeSeconds;
            if (goalErrorAddedSeconds > 0)
            {
                double addedTime = absoluteCompensationChange;
                compensationChangeSeconds = Math.Min(addedTime, goalErrorAddedSeconds);
            }
            else
            {
                double removedTime = -absoluteCompensationChange;
                if (goalErrorAddedSeconds < -_MaximumSecondsBehindBeforeJumpForward)
                {
                    double multiplier = goalErrorAddedSeconds / -_MaximumSecondsBehindBeforeJumpForward;
                    multiplier = Math.Min(_MinimumCatchupMultiplier, multiplier);
                    removedTime *= multiplier;
                }
                compensationChangeSeconds = Math.Max(removedTime, goalErrorAddedSeconds);
            }

            DurationMsg compensationChange = RosTimeHelper.FromSec(compensationChangeSeconds);

            DurationMsg totalAddedTime = RosTimeHelper.Add(timeSinceLastReceivedValue, compensationChange);

            // When attempting to go back in time, allow time to progress forward but by a smaller amount
            double totalAddedTimeSeconds = RosTimeHelper.ToSec(totalAddedTime);
            double minimumAllowedTotalSeconds = _SmallestMultipleTimeStep * timeSinceLastReceivedValueSeconds;
            totalAddedTimeSeconds = Math.Max(totalAddedTimeSeconds, minimumAllowedTotalSeconds);
            totalAddedTime = RosTimeHelper.FromSec(totalAddedTimeSeconds);

            if (totalAddedTime.sec == 0 && totalAddedTime.nanosec == 0)
            {
                totalAddedTime.nanosec = 1;
            }

            TimeMsg currentEstimate = RosTimeHelper.Add(latestTimeEstimate, totalAddedTime);
            return currentEstimate;
        }

    }
}
