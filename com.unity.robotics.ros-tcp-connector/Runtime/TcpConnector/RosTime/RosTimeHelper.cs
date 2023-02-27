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

        public static float _MaximumDeltaTime = 0.33f;

        private static RosTimeHelper _instance = null;

        public static RosTimeHelper Instance
        {
            get
            {
                if (_instance == null)
                {
                    _instance = new RosTimeHelper();
                }
                return _instance;
            }
        }

        private ScaledTimeEstimator ScaledTimeEstimate
        {
            get;
            set;
        }

        private WallTimeOffsetEstimator WallTimeOffsetEstimate
        {
            get;
            set;
        }

        public ExternalTimeTracker WallTimeTracker
        {
            get;
            private set;
        }

        public ExternalTimeTracker ExternalClockTimeTracker
        {
            get;
            private set;
        }

        private TimeMsg startTimeOfFrameExternalClock = new TimeMsg(0, 0);
        private TimeMsg startTimeOfFrameWall = new TimeMsg(0, 0);
        private int frameCountOfFixedUpdateTime = -1;

        private float measuredTimeOfFrameStartUnity = 0.0f;

        private TimeMsg wallTimeAtFrameStart = new TimeMsg(0, 0);
        private TimeMsg externalClockTimeAtFrameStart = new TimeMsg(0, 0);

        private RosTimeHelper()
        {
            WallTimeOffsetEstimate = new WallTimeOffsetEstimator();
            WallTimeTracker = new ExternalTimeTracker(WallTimeOffsetEstimate);

            ScaledTimeEstimate = new ScaledTimeEstimator();
            ExternalClockTimeTracker = new ExternalTimeTracker(ScaledTimeEstimate);
        }

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

        public float TimeScale => ScaledTimeEstimate.TimeScale;

        public bool IsPaused => ScaledTimeEstimate.IsPaused;

        public static int ClockInfoUpdateCount
        {
            get;
            private set;
        } = 0;

        public void OnRosConnectionEstablished()
        {
            WallTimeTracker.Reset();
            ExternalClockTimeTracker.Reset();
        }

        public void OnRosConnectionLost()
        {
            WallTimeTracker.Reset();
            ExternalClockTimeTracker.Reset();
        }

        public void OnSysCommandClockInfoReceived(SysCommand_ClockInfo sysCommandClockInfo)
        {

            TimeMsg receivedWallTime = new TimeMsg(sysCommandClockInfo.wall_secs, sysCommandClockInfo.wall_nsecs);
            TimeMsg receivedClockTime = new TimeMsg(sysCommandClockInfo.clock_secs, sysCommandClockInfo.clock_nsecs);

            bool resetClockTime = sysCommandClockInfo.should_reset_clock_time
                                  || IsPaused != sysCommandClockInfo.is_paused; //A change in paused state.

            ScaledTimeEstimate.UpdateTimeParameters(sysCommandClockInfo.time_scale, sysCommandClockInfo.is_paused);
            WallTimeOffsetEstimate.UpdateTimeParameters(sysCommandClockInfo.time_scale, sysCommandClockInfo.is_paused);

            WallTimeTracker.OnNewValueReceived(receivedWallTime, resetClockTime);
            ExternalClockTimeTracker.OnNewValueReceived(receivedClockTime, resetClockTime);

            ClockInfoUpdateCount++;
        }

        public void OnFixedUpdate(int frameCount)
        {

            TimeMsg currentExternalClockTimeEstimate = ScaledTimeEstimate.UpdateAndGetEstimation();
            TimeMsg currentWallTimeEstimate = WallTimeOffsetEstimate.UpdateAndGetEstimation();

            if (frameCountOfFixedUpdateTime != frameCount)
            {
                //Only grab the first fixed update of the frame.
                frameCountOfFixedUpdateTime = frameCount;
                startTimeOfFrameExternalClock = currentExternalClockTimeEstimate;
                startTimeOfFrameWall = currentWallTimeEstimate;
            }
        }

        public void OnRegularUpdate(float unityTime, int frameCount)
        {

            _MaximumDeltaTime = Time.maximumDeltaTime;

            TimeMsg scaledTimeExternalClock = ScaledTimeEstimate.UpdateAndGetEstimation();
            TimeMsg scaledTimeWall = WallTimeOffsetEstimate.UpdateAndGetEstimation();

            if (WallTimeTracker.AnyMessagesReceived)
            {
                wallTimeAtFrameStart = WallTimeTracker.GetCurrentEstimate(scaledTimeWall);
            }
            else
            {
                Debug.Log("No Wall Time Messages received...");
                wallTimeAtFrameStart = GetEpochWallTime();
            }

            externalClockTimeAtFrameStart = ExternalClockTimeTracker.GetCurrentEstimate(scaledTimeExternalClock);
            measuredTimeOfFrameStartUnity = unityTime;

            if (frameCount == frameCountOfFixedUpdateTime)
            {
                //There was a fixed update this frame, we can account for the additional delay processing the fixed update.
                DurationMsg durationSinceStartOfFrameExternalClock = FromTo(startTimeOfFrameExternalClock, scaledTimeExternalClock);
                float secondsSinceStartOfFrameExternalClock = (float) ToSec(durationSinceStartOfFrameExternalClock);
                secondsSinceStartOfFrameExternalClock = Mathf.Min(secondsSinceStartOfFrameExternalClock, Time.maximumDeltaTime);

                //Debug.Log($"There was a fixed update this frame: secondsSinceStartOfFrame = {secondsSinceStartOfFrame}");

                measuredTimeOfFrameStartUnity -= secondsSinceStartOfFrameExternalClock;
                DurationMsg startOfFrameAddedDuration = FromSec(-secondsSinceStartOfFrameExternalClock);
                wallTimeAtFrameStart = Add(wallTimeAtFrameStart, startOfFrameAddedDuration);
                externalClockTimeAtFrameStart = Add(externalClockTimeAtFrameStart, startOfFrameAddedDuration);
            }

            Debug.Log($"OnRegularUpdate, Time.timeMS = {((Time.time % 1) * 1000)}, measuredTimeOfFrameStartUnityMS = {((measuredTimeOfFrameStartUnity % 1) * 1000)}, frameCount = {frameCount}");

        }

        private DurationMsg GetUnityDurationSinceLastStoredTime(float unityTime)
        {
            float unitySecondsSinceLastStoredTime = unityTime - measuredTimeOfFrameStartUnity;
            DurationMsg unityDurationSinceLastStoredTime = FromSec(unitySecondsSinceLastStoredTime);
            return unityDurationSinceLastStoredTime;
        }

        public TimeMsg GetExternalSimulatedTime(float unityTime)
        {
            DurationMsg offset = GetUnityDurationSinceLastStoredTime(unityTime);
            TimeMsg result = Add(externalClockTimeAtFrameStart, offset);
            return result;
        }

        public TimeMsg GetRosWallTime(float unityTime)
        {
            DurationMsg offset = GetUnityDurationSinceLastStoredTime(unityTime);
            Debug.Log($"wallTimeAtFrameStartMS = {wallTimeAtFrameStart.nanosec / 1e6}");
            Debug.Log($"offsetMS = {offset.nanosec / 1e6}");
            TimeMsg result = Add(wallTimeAtFrameStart, offset);
            Debug.Log($"resultMS = {result.nanosec / 1e6}");
            return result;
        }

        public TimeMsg GetEpochWallTime()
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

        public static TimeMsg Add(TimeMsg timeMsg, double addedSeconds)
        {
            return Add(timeMsg, FromSec(addedSeconds));
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
