using System;
using System.Runtime.Serialization;
using JetBrains.Annotations;

namespace Unity.Robotics.ROSTCPConnector.RosService
{
    public class RosServiceErrorException : RosConnectionException
    {
        public RosServiceErrorException()
        {
        }

        protected RosServiceErrorException([NotNull] SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

        public RosServiceErrorException(string message) : base(message)
        {
        }

        public RosServiceErrorException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
