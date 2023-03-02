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

        private object concurrencyLock = new object();

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
            TimeMsg result;
            lock (concurrencyLock)
            {
                if (!setupPerformed) PerformSetup();

                bool isPaused = IsPaused;
                float timeScale = TimeScale;

                DateTime now = DateTime.Now;
                if (!isPaused && timeScale > 0)
                {
                    TimeSpan timeSinceLastUpdate = now - timeOfLastEstimationUpdate;
                    double totalSecondsSinceLastUpdate = timeSinceLastUpdate.TotalSeconds;
                    if (totalSecondsSinceLastUpdate > MaximumDeltaTime)
                    {
                        //Add a limit to the amount estimation can progress in a single update.
                        totalSecondsSinceLastUpdate = MaximumDeltaTime;
                    }

                    double scaledPassedTime = totalSecondsSinceLastUpdate * timeScale;
                    DurationMsg asDurationMessage = DurationMsg.FromSec(scaledPassedTime);
                    timeEstimate += asDurationMessage;
                }

                timeOfLastEstimationUpdate = now;
                result = timeEstimate;
            }
            return result;
        }

        public void UpdateTimeParameters(float newTimeScale, bool newIsPaused)
        {
            UpdateAndGetEstimation();
            this._timeScale = newTimeScale;
            this._isPaused = newIsPaused;
        }


    }
}
