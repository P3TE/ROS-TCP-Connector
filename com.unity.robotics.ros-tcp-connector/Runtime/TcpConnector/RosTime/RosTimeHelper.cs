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

        public static int _NanoSecondsPerSecond = 1000 * 1000 * 1000;

        public static float _MaximumDeltaTime = 0.33f;

        public enum RosTimeType
        {
            PublishClock = 0,
            UseWallTime = 1,
            UseExternalClock = 2
        }

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

        private float unityTimeStartOfFrameWall = 0.0f;
        private float unityTimeStartOfFrameExternalClock = 0.0f;

        private TimeMsg wallTimeAtFrameStart = new TimeMsg(0, 0);
        private TimeMsg externalClockTimeAtFrameStart = new TimeMsg(0, 0);

        public bool EditorSyncAllTimeTypes
        {
            get;
            set;
        } = false;

        private RosTimeHelper()
        {
            WallTimeOffsetEstimate = new WallTimeOffsetEstimator();
            WallTimeTracker = new ExternalTimeTracker(WallTimeOffsetEstimate);

            ScaledTimeEstimate = new ScaledTimeEstimator();
            ExternalClockTimeTracker = new ExternalTimeTracker(ScaledTimeEstimate);
        }



        private RosTimeType _currentTimeHandling = RosTimeType.PublishClock;

        public RosTimeType CurrentTimeHandling
        {
            get => _currentTimeHandling;
            set
            {
                _currentTimeHandling = value;
            }
        }

        public float TimeScale => ScaledTimeEstimate.TimeScale;

        public bool IsPaused => ScaledTimeEstimate.IsPaused;

        public bool SyncWallTime
        {
            get
            {
#if UNITY_EDITOR
                if (EditorSyncAllTimeTypes) return true;
#endif
                return CurrentTimeHandling == RosTimeType.UseWallTime;
            }
        }

        public bool SyncExternalClock
        {
            get
            {
#if UNITY_EDITOR
                if (EditorSyncAllTimeTypes) return true;
#endif
                return CurrentTimeHandling == RosTimeType.UseExternalClock;
            }
        }

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

            if (SyncWallTime)
            {
                TimeMsg receivedWallTime = new TimeMsg(sysCommandClockInfo.wall_secs, sysCommandClockInfo.wall_nsecs);
                WallTimeOffsetEstimate.UpdateTimeParameters(sysCommandClockInfo.time_scale, sysCommandClockInfo.is_paused);
                WallTimeTracker.OnNewValueReceived(receivedWallTime, false);
            }

            if (SyncExternalClock)
            {

                ScaledTimeEstimate.UpdateTimeParameters(sysCommandClockInfo.time_scale, sysCommandClockInfo.is_paused);

                TimeMsg receivedClockTime = new TimeMsg(sysCommandClockInfo.clock_secs, sysCommandClockInfo.clock_nsecs);

                bool resetClockTime = sysCommandClockInfo.should_reset_clock_time
                                      || IsPaused != sysCommandClockInfo.is_paused; //A change in paused state.

                ExternalClockTimeTracker.OnNewValueReceived(receivedClockTime, resetClockTime);
            }

            ClockInfoUpdateCount++;
        }

        public void OnFixedUpdate(int frameCount)
        {

            if (frameCountOfFixedUpdateTime == frameCount)
            {
                return;
            }
            frameCountOfFixedUpdateTime = frameCount;

            if (SyncWallTime)
            {
                TimeMsg currentWallTimeEstimate = WallTimeOffsetEstimate.UpdateAndGetEstimation();
                startTimeOfFrameWall = currentWallTimeEstimate;
            }

            if (SyncExternalClock)
            {
                TimeMsg currentExternalClockTimeEstimate = ScaledTimeEstimate.UpdateAndGetEstimation();
                startTimeOfFrameExternalClock = currentExternalClockTimeEstimate;
            }
        }

        private float GetFixedUpdateOffset(TimeMsg startTimeOfFrame, TimeMsg currentTimeOfFrame, int frameCount)
        {

            if (frameCount != frameCountOfFixedUpdateTime)
            {
                //Update was the first thing in this frame, assume no offset.
                return 0f;
            }

            //There was a fixed update this frame, we can account for the additional delay processing the fixed update.
            DurationMsg durationSinceStartOfFrameExternalClock = currentTimeOfFrame - startTimeOfFrame;
            float secondsSinceStartOfFrame = (float) durationSinceStartOfFrameExternalClock.ToSec();
            secondsSinceStartOfFrame = Mathf.Min(secondsSinceStartOfFrame, Time.maximumDeltaTime);
            return secondsSinceStartOfFrame;
        }

        public void OnRegularUpdate(float unityTime, int frameCount)
        {

            _MaximumDeltaTime = Time.maximumDeltaTime;
            measuredTimeOfFrameStartUnity = unityTime;


            if (SyncWallTime)
            {
                TimeMsg scaledTimeWall = WallTimeOffsetEstimate.UpdateAndGetEstimation();

                if (WallTimeTracker.AnyMessagesReceived)
                {
                    wallTimeAtFrameStart = WallTimeTracker.GetCurrentEstimate(scaledTimeWall);
                }
                else
                {
                    wallTimeAtFrameStart = GetEpochWallTime();
                }

                float wallSecondsSinceStartOfFrame =
                    GetFixedUpdateOffset(startTimeOfFrameWall, wallTimeAtFrameStart, frameCount);
                unityTimeStartOfFrameWall = unityTime - wallSecondsSinceStartOfFrame;
                wallTimeAtFrameStart -= DurationMsg.FromSec(wallSecondsSinceStartOfFrame);
            }

            if (SyncExternalClock)
            {
                TimeMsg scaledTimeExternalClock = ScaledTimeEstimate.UpdateAndGetEstimation();
                externalClockTimeAtFrameStart = ExternalClockTimeTracker.GetCurrentEstimate(scaledTimeExternalClock);
                float clockSecondsSinceStartOfFrame =
                    GetFixedUpdateOffset(startTimeOfFrameExternalClock, externalClockTimeAtFrameStart, frameCount);
                unityTimeStartOfFrameExternalClock = unityTime - clockSecondsSinceStartOfFrame;
                externalClockTimeAtFrameStart -= DurationMsg.FromSec(clockSecondsSinceStartOfFrame);
            }

        }

        public TimeMsg GetExternalSimulatedTime(float unityTime)
        {
            DurationMsg offset = DurationMsg.FromSec(unityTime - unityTimeStartOfFrameExternalClock);
            TimeMsg result = externalClockTimeAtFrameStart + offset;
            return result;
        }

        public TimeMsg GetRosWallTime(float unityTime)
        {
            DurationMsg offset = DurationMsg.FromSec(unityTime - unityTimeStartOfFrameWall);
            TimeMsg result = wallTimeAtFrameStart + offset;
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

    }
}
