using System;
using System.Runtime.Serialization;
using JetBrains.Annotations;

namespace Unity.Robotics.ROSTCPConnector
{
    public class NoRosConnectionException : RosConnectionException
    {
        public NoRosConnectionException()
        {
        }

        protected NoRosConnectionException([NotNull] SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

        public NoRosConnectionException(string message) : base(message)
        {
        }

        public NoRosConnectionException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
