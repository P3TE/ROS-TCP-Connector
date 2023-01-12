using System;
using System.Runtime.Serialization;
using JetBrains.Annotations;

namespace Unity.Robotics.ROSTCPConnector
{
    public class RosConnectionException : Exception
    {
        public RosConnectionException()
        {
        }

        protected RosConnectionException([NotNull] SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

        public RosConnectionException(string message) : base(message)
        {
        }

        public RosConnectionException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
