using System;
using RosMessageTypes.BuiltinInterfaces;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector.RosTime
{
    public class ScaledTimeEstimator
    {
        private TimeMsg timeEstimate = new TimeMsg(0, 0);

        private float timeScale = 1.0f;
        private bool isPaused = false;

        private DateTime timeOfLastEstimationUpdate = DateTime.Now;
        private bool setupPerformed = false;

        public float TimeScale => timeScale;

        public bool IsPaused => isPaused;

        private void PerformSetup()
        {
            setupPerformed = true;
            timeOfLastEstimationUpdate = DateTime.Now;
            timeEstimate = new TimeMsg(0, 0);
        }

        public TimeMsg UpdateAndGetEstimation()
        {
            if (!setupPerformed) PerformSetup();

            DateTime now = DateTime.Now;
            if (!isPaused && timeScale > 0)
            {
                TimeSpan timeSinceLastUpdate = now - timeOfLastEstimationUpdate;
                double totalSecondsSinceLastUpdate = timeSinceLastUpdate.TotalSeconds;
                if (totalSecondsSinceLastUpdate > RosTimeHelper._MaximumDeltaTime)
                {
                    //Add a limit to the amount estimation can progress in a single update.
                    totalSecondsSinceLastUpdate = RosTimeHelper._MaximumDeltaTime;
                }
                double scaledPassedTime = totalSecondsSinceLastUpdate * timeScale;
                DurationMsg asDurationMessage = RosTimeHelper.FromSec(scaledPassedTime);
                timeEstimate = RosTimeHelper.Add(timeEstimate, asDurationMessage);
            }

            timeOfLastEstimationUpdate = now;
            return timeEstimate;
        }

        public void UpdateTimeParameters(float newTimeScale, bool newIsPaused)
        {
            UpdateAndGetEstimation();
            this.timeScale = newTimeScale;
            this.isPaused = newIsPaused;
        }


    }
}
