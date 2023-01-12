using System;
using System.Runtime.Serialization;
using JetBrains.Annotations;

namespace Unity.Robotics.ROSTCPConnector.RosService
{
    public class RosServiceFailedException : RosConnectionException
    {
        public RosServiceFailedException()
        {
        }

        protected RosServiceFailedException([NotNull] SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

        public RosServiceFailedException(string message) : base(message)
        {
        }

        public RosServiceFailedException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
