using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using RosMessageTypes.BuiltinInterfaces;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector.RosTime
{
    public class RosTimeHelper
    {
        private const int _ClockQueueMaxLength = 5;

        public enum RosTimeType
        {
            PublishClock = 0,
            UseWallTime = 1,
            UseExternalClock = 2
        }

        private static RosTimeType _currentTimeHandling = RosTimeType.PublishClock;

        public static RosTimeType CurrentTimeHandling
        {
            get => _currentTimeHandling;
            set
            {
                _currentTimeHandling = value;
                //TODO - Update the endpoint...
            }
        }

        public static TimeTracker WallTimeTracker
        {
            get;
        } = new TimeTracker();
        private static TimeTracker ExternalClockTimeTracker = new TimeTracker();

        private static bool timeTrackersInitialised = false;

        public class TimeTracker
        {

            public DateTime receivedTimeOfPreviousValue;

            private TimeMsg previousValue = null;
            private TimeMsg goalValue = null;

            public double lerpTime = 1.0f;
            public bool useLinearInterpolation = true;

            public float timeScale = 1.0f;

            public bool jumpToNextValue = true;

            public void Reset()
            {
                jumpToNextValue = true;
                if (previousValue == null)
                {
                    JumpValueTo(new TimeMsg(0, 0));
                }
            }

            public void OnNewValueReceived(TimeMsg newGoalValue, bool resetTime = false)
            {
                if (resetTime)
                {
                    JumpValueTo(newGoalValue);
                    return;
                }

                if (jumpToNextValue)
                {
                    jumpToNextValue = false;
                    JumpValueTo(newGoalValue);
                    return;
                }

                this.previousValue = GetCurrentValue();
                this.goalValue = newGoalValue;
                this.receivedTimeOfPreviousValue = DateTime.Now;
            }

            public void JumpValueTo(TimeMsg newTime)
            {
                previousValue = newTime;
                goalValue = newTime;
                receivedTimeOfPreviousValue = DateTime.Now;
            }

            public TimeMsg GetCurrentValue()
            {
                if (previousValue == null)
                {
                    return new TimeMsg(0, 0);
                }
                DateTime currentTime = DateTime.Now;
                TimeSpan timeSinceLastValueReceived = currentTime - receivedTimeOfPreviousValue;

                double realTimeSinceLastValueReceivedSeconds = timeSinceLastValueReceived.TotalSeconds;
                double scaledTimeSinceLastValueReceivedSeconds = realTimeSinceLastValueReceivedSeconds * timeScale;

                DurationMsg scaledDurationSinceLastValueReceived = FromSec(scaledTimeSinceLastValueReceivedSeconds);

                DurationMsg fromPreviousToGoal = FromTo(previousValue, goalValue);
                double fromPreviousToGoalTotalSeconds = ToSec(fromPreviousToGoal);

                double t = realTimeSinceLastValueReceivedSeconds / lerpTime;
                double interpolationMultiplier = CalculateInterpolationMultiplier(t);
                double movementTowardsGoalTimeSeconds = fromPreviousToGoalTotalSeconds * interpolationMultiplier;
                DurationMsg movementTowardsGoalTime = FromSec(movementTowardsGoalTimeSeconds);

                // Add the movement towards the goal time.
                TimeMsg currentTimeValue = Add(previousValue, movementTowardsGoalTime);
                // Add time since the message was received.
                currentTimeValue = Add(currentTimeValue, scaledDurationSinceLastValueReceived);

                return currentTimeValue;
            }

            public DurationMsg GetFromCurrentToGoal()
            {
                if (previousValue == null) return new DurationMsg(0, 0);
                return FromTo(previousValue, goalValue);
            }

            public TimeMsg GetGoalWithExtrapolation()
            {
                if (previousValue == null)
                {
                    return new TimeMsg(0, 0);
                }
                DateTime currentTime = DateTime.Now;
                TimeSpan timeSinceLastValueReceived = currentTime - receivedTimeOfPreviousValue;

                double realTimeSinceLastValueReceivedSeconds = timeSinceLastValueReceived.TotalSeconds;
                double scaledTimeSinceLastValueReceivedSeconds = realTimeSinceLastValueReceivedSeconds * timeScale;

                DurationMsg scaledDurationSinceLastValueReceived = FromSec(scaledTimeSinceLastValueReceivedSeconds);

                // Add time since the message was received.
                TimeMsg extrapolatedGoalTime = Add(goalValue, scaledDurationSinceLastValueReceived);

                return extrapolatedGoalTime;
            }

            private double CalculateInterpolationMultiplier(double t)
            {
                if (t < 0) return 0.0f;
                if (t > 1) return 1.0f;

                if (useLinearInterpolation)
                {
                    return t;
                }
                else
                {
                    //SmoothStep
                    double tSquared = t * t;
                    double tCubed = tSquared * t;
                    double multiplier = (-2 * tCubed) + (3 * tSquared);
                    return multiplier;
                }
            }
        }



        public static bool ExternalClockIsPaused
        {
            get;
            private set;
        }

        public static float ExternalClockTimeScale
        {
            get;
            private set;
        } = 1.0f;

        public static int ClockInfoUpdateCount
        {
            get;
            private set;
        } = 0;

        public static void OnRosConnectionEstablished()
        {
            InitialiseTimeTrackers();
        }

        private static void InitialiseTimeTrackers()
        {
            WallTimeTracker.Reset();
            ExternalClockTimeTracker.Reset();
        }

        private static void InitialiseTimeTrackersIfApplicable()
        {
            if (timeTrackersInitialised)
            {
                return;
            }
            timeTrackersInitialised = true;
            InitialiseTimeTrackers();
        }

        public static void OnSysCommandClockInfoReceived(SysCommand_ClockInfo sysCommandClockInfo)
        {

            TimeMsg receivedWallTime = new TimeMsg(sysCommandClockInfo.wall_secs, sysCommandClockInfo.wall_nsecs);
            TimeMsg receivedClockTime = new TimeMsg(sysCommandClockInfo.clock_secs, sysCommandClockInfo.clock_nsecs);

            WallTimeTracker.OnNewValueReceived(receivedWallTime);

            bool resetClockTime = sysCommandClockInfo.should_reset_clock_time || sysCommandClockInfo.is_paused;
            ExternalClockTimeTracker.OnNewValueReceived(receivedClockTime, resetClockTime);

            ExternalClockIsPaused = sysCommandClockInfo.is_paused;
            ExternalClockTimeScale = sysCommandClockInfo.time_scale;
            ExternalClockTimeTracker.timeScale = sysCommandClockInfo.time_scale;

            ClockInfoUpdateCount++;
        }

        private static DateTime startTimeOfFrame = DateTime.Now;
        private static int frameCountOfFixedUpdateTime = -1;

        private static float measuredTimeOfFrameStartUnity = 0.0f;

        private static TimeMsg wallTimeAtFrameStart = new TimeMsg(0, 0);
        private static TimeMsg clockTimeAtFrameStart = new TimeMsg(0, 0);

        public static void OnFixedUpdate(int frameCount)
        {
            InitialiseTimeTrackersIfApplicable();
            Debug.Log($"OnFixedUpdate, Time.timeMS = {((Time.time % 1) * 1000)}, frameCount = {frameCount}");
            if (frameCountOfFixedUpdateTime != frameCount)
            {
                //Only grab the first fixed update of the frame.
                frameCountOfFixedUpdateTime = frameCount;
                startTimeOfFrame = DateTime.Now;
            }
        }

        public static void OnRegularUpdate(float unityTime, int frameCount)
        {
            InitialiseTimeTrackersIfApplicable();
            DateTime currentTime = DateTime.Now;

            wallTimeAtFrameStart = WallTimeTracker.GetCurrentValue();
            clockTimeAtFrameStart = ExternalClockTimeTracker.GetCurrentValue();
            measuredTimeOfFrameStartUnity = unityTime;

            if (frameCount == frameCountOfFixedUpdateTime)
            {
                //There was a fixed update this frame, we can account for the additional delay processing the fixed update.
                float secondsSinceStartOfFrame = (float) (currentTime - startTimeOfFrame).TotalSeconds;
                secondsSinceStartOfFrame = Mathf.Min(secondsSinceStartOfFrame, Time.maximumDeltaTime);

                //Debug.Log($"There was a fixed update this frame: secondsSinceStartOfFrame = {secondsSinceStartOfFrame}");

                measuredTimeOfFrameStartUnity -= secondsSinceStartOfFrame;
                DurationMsg startOfFrameAddedDuration = FromSec(-secondsSinceStartOfFrame);
                wallTimeAtFrameStart = Add(wallTimeAtFrameStart, startOfFrameAddedDuration);
                clockTimeAtFrameStart = Add(clockTimeAtFrameStart, startOfFrameAddedDuration);
            }

            Debug.Log($"OnRegularUpdate, Time.timeMS = {((Time.time % 1) * 1000)}, measuredTimeOfFrameStartUnityMS = {((measuredTimeOfFrameStartUnity % 1) * 1000)}, frameCount = {frameCount}");

        }

        private static DurationMsg GetUnityDurationSinceLastStoredTime(float unityTime)
        {
            float unitySecondsSinceLastStoredTime = unityTime - measuredTimeOfFrameStartUnity;
            DurationMsg unityDurationSinceLastStoredTime = FromSec(unitySecondsSinceLastStoredTime);
            return unityDurationSinceLastStoredTime;
        }

        public static TimeMsg GetExternalSimulatedTime(float unityTime)
        {
            DurationMsg offset = GetUnityDurationSinceLastStoredTime(unityTime);
            TimeMsg result = Add(clockTimeAtFrameStart, offset);
            return result;
        }

        public static TimeMsg GetRosWallTime(float unityTime)
        {
            DurationMsg offset = GetUnityDurationSinceLastStoredTime(unityTime);
            Debug.Log($"wallTimeAtFrameStartMS = {wallTimeAtFrameStart.nanosec / 1e6}");
            Debug.Log($"offsetMS = {offset.nanosec / 1e6}");
            TimeMsg result = Add(wallTimeAtFrameStart, offset);
            Debug.Log($"resultMS = {result.nanosec / 1e6}");
            return result;
        }

        public static TimeMsg GetEpochWallTime()
        {
            DateTime epochUtc = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            long ticksSinceEpochUtc = DateTime.UtcNow.Ticks - epochUtc.Ticks;

            long secondsSinceEpoch = ticksSinceEpochUtc / TimeSpan.TicksPerSecond;

            long subSecondTicks = ticksSinceEpochUtc - (secondsSinceEpoch * TimeSpan.TicksPerSecond);
            long nanoSeconds = subSecondTicks * 100;

#if ROS2
            return new TimeMsg((int)secondsSinceEpoch, (uint)nanoSeconds);
#else
            return new TimeMsg((uint)secondsSinceEpoch, (uint)nanoSeconds);
#endif
        }

        public static DurationMsg FromTo(TimeMsg from, TimeMsg to)
        {
            int secDifference = ((int)to.sec) - ((int)from.sec);
            int nanoSecondDifference = ((int)to.nanosec) - ((int)from.nanosec);
            if (nanoSecondDifference < 0)
            {
                secDifference--;
                nanoSecondDifference += 1000000000;
            }
#if ROS2
            return new DurationMsg(secDifference, (uint) nanoSecondDifference);
#else
            return new DurationMsg(secDifference, nanoSecondDifference);
#endif
        }

        public static TimeMsg Add(TimeMsg timeMsg, DurationMsg addedDuration)
        {
            uint resultSecs = (uint) (timeMsg.sec + addedDuration.sec);
            uint resultNsecs = timeMsg.nsecs + (uint)addedDuration.nanosec;
            const uint _NanoSeconsPerSecond = 1000 * 1000 * 1000;
            if (resultNsecs >= _NanoSeconsPerSecond)
            {
                resultNsecs -= _NanoSeconsPerSecond;
                resultSecs += 1;
            }
#if ROS2
            return new TimeMsg((int)resultSecs, resultNsecs);
#else
            return new TimeMsg(resultSecs, resultNsecs);
#endif
        }

        public static double ToSec(DurationMsg durationMsg)
        {
            return durationMsg.sec + (durationMsg.nanosec* 0.000000001);
        }

        public static DurationMsg FromSec(double totalSeconds)
        {
            int totalSecondsInt = (int)totalSeconds;
            double remainder = totalSeconds - totalSecondsInt;
            if (remainder < 0)
            {
                totalSecondsInt--;
                remainder = 1.0 + remainder;
            }

            uint remainderNanoSecondsInt = (uint) (remainder * 1e9);
#if ROS2
            return new DurationMsg(totalSecondsInt, remainderNanoSecondsInt);
#else
            return new DurationMsg(totalSecondsInt, (int)remainderNanoSecondsInt);
#endif
        }


    }
}
