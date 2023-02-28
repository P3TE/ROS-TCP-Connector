using System;
using RosMessageTypes.BuiltinInterfaces;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector.RosTime
{
    public class ScaledTimeEstimator
    {
        private TimeMsg timeEstimate = new TimeMsg(0, 0);

        private float _timeScale = 1.0f;
        private bool _isPaused = false;

        private DateTime timeOfLastEstimationUpdate = DateTime.Now;
        private bool setupPerformed = false;

        public virtual float TimeScale => _timeScale;

        public virtual bool IsPaused => _isPaused;

        private void PerformSetup()
        {
            setupPerformed = true;
            timeOfLastEstimationUpdate = DateTime.Now;
            timeEstimate = new TimeMsg(0, 0);
        }

        public virtual float MaximumDeltaTime => RosTimeHelper._MaximumDeltaTime * 5;

        public TimeMsg UpdateAndGetEstimation()
        {
            if (!setupPerformed) PerformSetup();

            DateTime now = DateTime.Now;
            if (!IsPaused && TimeScale > 0)
            {
                TimeSpan timeSinceLastUpdate = now - timeOfLastEstimationUpdate;
                double totalSecondsSinceLastUpdate = timeSinceLastUpdate.TotalSeconds;
                if (totalSecondsSinceLastUpdate > MaximumDeltaTime)
                {
                    //Add a limit to the amount estimation can progress in a single update.
                    totalSecondsSinceLastUpdate = MaximumDeltaTime;
                }
                double scaledPassedTime = totalSecondsSinceLastUpdate * TimeScale;
                DurationMsg asDurationMessage = RosTimeHelper.FromSec(scaledPassedTime);
                timeEstimate = RosTimeHelper.Add(timeEstimate, asDurationMessage);
            }

            timeOfLastEstimationUpdate = now;
            return timeEstimate;
        }

        public void UpdateTimeParameters(float newTimeScale, bool newIsPaused)
        {
            UpdateAndGetEstimation();
            this._timeScale = newTimeScale;
            this._isPaused = newIsPaused;
        }


    }
}
