using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;

namespace RosMessageTypes.BuiltinInterfaces
{
    public class TimeMsg : Message
    {

        public const int _NanoSecondsPerSecond = 1000 * 1000 * 1000;

#if !ROS2
        public const string k_RosMessageName = "std_msgs/Time";
        public override string RosMessageName => k_RosMessageName;

        public uint secs;
        public uint nsecs;

        // for convenience when writing ROS2 agnostic code
        public int sec { get => (int)secs; set => secs = (uint)value; }
        public uint nanosec { get => nsecs; set => nsecs = value; }

        public TimeMsg()
        {
            this.secs = 0;
            this.nsecs = 0;
        }

        public TimeMsg(uint secs, uint nsecs)
        {
            this.secs = secs;
            this.nsecs = nsecs;
        }

        public static TimeMsg Deserialize(MessageDeserializer deserializer) => new TimeMsg(deserializer);

        TimeMsg(MessageDeserializer deserializer)
        {
            deserializer.Read(out this.secs);
            deserializer.Read(out this.nsecs);
        }

        public override void SerializeTo(MessageSerializer serializer)
        {
            serializer.Write(this.secs);
            serializer.Write(this.nsecs);
        }

        public override string ToString()
        {
            return "Time: " +
            "\nsecs: " + secs.ToString() +
            "\nnsecs: " + nsecs.ToString();
        }
#else
        public const string k_RosMessageName = "builtin_interfaces/Time";
        public override string RosMessageName => k_RosMessageName;

        //  This message communicates ROS Time defined here:
        //  https://design.ros2.org/articles/clock_and_time.html
        //  The seconds component, valid over all int32 values.
        public int sec;
        //  The nanoseconds component, valid in the range [0, 10e9).
        public uint nanosec;

        // for convenience when writing ROS2 agnostic code
        public uint secs { get => (uint)sec; set => sec = (int)value; }
        public uint nsecs { get => nsecs; set => nsecs = value; }

        public TimeMsg()
        {
            this.sec = 0;
            this.nanosec = 0;
        }

        public TimeMsg(int sec, uint nanosec)
        {
            this.sec = sec;
            this.nanosec = nanosec;
        }

        public static TimeMsg Deserialize(MessageDeserializer deserializer) => new TimeMsg(deserializer);

        TimeMsg(MessageDeserializer deserializer)
        {
            deserializer.Read(out this.sec);
            deserializer.Read(out this.nanosec);
        }

        public override void SerializeTo(MessageSerializer serializer)
        {
            serializer.Write(this.sec);
            serializer.Write(this.nanosec);
        }

        public override string ToString()
        {
            return "Time: " +
            "\nsec: " + sec.ToString() +
            "\nnanosec: " + nanosec.ToString();
        }
#endif

#if UNITY_EDITOR
        [UnityEditor.InitializeOnLoadMethod]
#else
        [UnityEngine.RuntimeInitializeOnLoadMethod]
#endif
        public static void Register()
        {
            MessageRegistry.Register(k_RosMessageName, Deserialize);
        }

        public static DurationMsg operator -(TimeMsg a, TimeMsg b)
        {
            int secDifference = ((int)a.sec) - ((int)b.sec);
            int nanoSecondDifference = ((int)a.nanosec) - ((int)b.nanosec);
            if (nanoSecondDifference < 0)
            {
                secDifference--;
                nanoSecondDifference += _NanoSecondsPerSecond;
            }
#if ROS2
            return new DurationMsg(secDifference, (uint) nanoSecondDifference);
#else
            return new DurationMsg(secDifference, nanoSecondDifference);
#endif
        }

        public static TimeMsg operator +(TimeMsg timeMsg, double addedSeconds)
        {
            return timeMsg + DurationMsg.FromSec(addedSeconds);
        }

        public static TimeMsg operator +(TimeMsg timeMsg, DurationMsg addedDuration)
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

        public static TimeMsg operator -(TimeMsg timeMsg, DurationMsg subtractedDuration)
        {
            int resultSecs = (int) timeMsg.sec - (int) subtractedDuration.sec;
            int resultNsecs = (int) timeMsg.nsecs - (int) subtractedDuration.nanosec;
            const int _NanoSeconsPerSecond = 1000 * 1000 * 1000;
            if (resultNsecs < 0)
            {
                resultNsecs += _NanoSeconsPerSecond;
                resultSecs -= 1;
            }
#if ROS2
            return new TimeMsg((int)resultSecs, (uint) resultNsecs);
#else
            return new TimeMsg((uint)resultSecs, (uint) resultNsecs);
#endif
        }
    }
}
