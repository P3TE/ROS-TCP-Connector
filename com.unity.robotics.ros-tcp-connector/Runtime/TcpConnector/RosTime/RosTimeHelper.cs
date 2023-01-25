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


        private class ClockData
        {
            public readonly SysCommand_ClockInfo sysCommandClockInfo;
            public readonly TimeMsg simWallTimeOfReceivedInfo;

            public ClockData(SysCommand_ClockInfo sysCommandClockInfo, TimeMsg simulatorWallTime)
            {
                this.sysCommandClockInfo = sysCommandClockInfo;
                this.simWallTimeOfReceivedInfo = simulatorWallTime;
            }

            public TimeMsg ClockTime => new TimeMsg(sysCommandClockInfo.clock_secs, sysCommandClockInfo.clock_nsecs);

            public TimeMsg EndpointWallTime => new TimeMsg(sysCommandClockInfo.wall_secs, sysCommandClockInfo.wall_nsecs);
        }

        private static object clockListLockObj = new object();

        private static LinkedList<ClockData> clockDataQueue = new LinkedList<ClockData>();

        public static void OnSysCommandClockInfoReceived(SysCommand_ClockInfo sysCommandClockInfo)
        {
            lock (clockListLockObj)
            {
                if (clockDataQueue.Count > _ClockQueueMaxLength)
                {
                    clockDataQueue.RemoveLast();
                }

                TimeMsg simulatorWallTime = GetSimulatorWallTime();
                ClockData clockData = new ClockData(sysCommandClockInfo, simulatorWallTime);
                clockDataQueue.AddFirst(clockData);
            }
        }

        private static bool TryGetMostRecentClockData(out ClockData clockData)
        {
            clockData = null;
            lock (clockListLockObj)
            {
                if (clockDataQueue.Count > 0)
                {
                    clockData = clockDataQueue.First.Value;
                }
            }

            return clockData != null;
        }

        public static TimeMsg GetExternalSimulatedTime()
        {
            if (!TryGetMostRecentClockData(out ClockData clockData))
            {
                Debug.LogWarning("No clock data received from the ROS-TCP-Endpoint, defaulting to 0.");
                return new TimeMsg(0, 0);
            }

            TimeMsg simulatorWallTime = GetSimulatorWallTime();

            DurationMsg additionalTime = FromTo(clockData.simWallTimeOfReceivedInfo, simulatorWallTime);
            TimeMsg result = Add(clockData.ClockTime, additionalTime);

            return result;
        }

        public static TimeMsg GetRosWallTime()
        {
            TimeMsg simulatorWallTime = GetSimulatorWallTime();
            return simulatorWallTime;
            //TODO - Implement properly.

            if (!TryGetMostRecentClockData(out ClockData clockData))
            {
                Debug.LogWarning("No clock data received from the ROS-TCP-Endpoint, defaulting to simulated wall time.");
                return simulatorWallTime;
            }

            DurationMsg additionalTime = FromTo(clockData.simWallTimeOfReceivedInfo, simulatorWallTime);
            TimeMsg result = Add(clockData.EndpointWallTime, additionalTime);

            return result;
        }

        public static TimeMsg GetSimulatorWallTime()
        {
            DateTime epochUtc = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            long ticksSinceEpochUtc = DateTime.UtcNow.Ticks - epochUtc.Ticks;

            long secondsSinceEpoch = ticksSinceEpochUtc / TimeSpan.TicksPerSecond;

            long subSecondTicks = ticksSinceEpochUtc - (secondsSinceEpoch * TimeSpan.TicksPerSecond);
            long nanoSeconds = subSecondTicks * 100;

            return new TimeMsg((uint)secondsSinceEpoch, (uint)nanoSeconds);
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
            return new TimeMsg(resultSecs, resultNsecs);
        }

        public static double ToSec(DurationMsg durationMsg)
        {
            return durationMsg.sec + (durationMsg.nanosec* 0.000000001);
        }


    }
}
