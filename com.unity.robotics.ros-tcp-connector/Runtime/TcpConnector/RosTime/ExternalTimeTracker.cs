using System;
using RosMessageTypes.BuiltinInterfaces;

namespace Unity.Robotics.ROSTCPConnector.RosTime
{
    public class ExternalTimeTracker
    {
        private const double _MaximumDeviationSecondsBeforeJump = 1.0f;
        private const double _MaximumCompensationChangeSecondsPerSecond = 0.025f;

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
            if (Math.Abs(fromOurEstimateToNewMessageSeconds) > _MaximumDeviationSecondsBeforeJump)
            {
                // We are REALLY far off
                // Jump immediately to the received value.
                jumpToNewValue = true;
            }
            else
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

            TimeMsg latestPlusElapsedTime = RosTimeHelper.Add(latestTimeEstimate, timeSinceLastReceivedValue);


            // Compensate for errors in estimation by moving towards the current goal value.
            double compensationChangeSeconds = Math.Sign(goalErrorAddedSeconds) * timeSinceLastReceivedValueSeconds * _MaximumCompensationChangeSecondsPerSecond;
            double absGoalErrorAddedSeconds = Math.Abs(goalErrorAddedSeconds);
            compensationChangeSeconds = Math.Clamp(compensationChangeSeconds, -absGoalErrorAddedSeconds, absGoalErrorAddedSeconds);
            DurationMsg compensationChange = RosTimeHelper.FromSec(compensationChangeSeconds);

            TimeMsg currentEstimate = RosTimeHelper.Add(latestPlusElapsedTime, compensationChange);
            return currentEstimate;
        }

    }
}
