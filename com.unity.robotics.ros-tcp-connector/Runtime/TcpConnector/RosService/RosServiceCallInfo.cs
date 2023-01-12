using System;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;

namespace Unity.Robotics.ROSTCPConnector.RosService
{
    public abstract class RosServiceCallInfoBase
    {
        public readonly RosTopicState topicState;

        public readonly int serviceId;
        public readonly TaskPauser taskPauser;

        public readonly Message messageToSend;

        public Action<Exception> serviceCallFailureAction;

        protected RosServiceCallInfoBase(RosTopicState topicState, Message messageToSend,
            Action<Exception> serviceCallFailureAction)
        {
            this.topicState = topicState;
            this.messageToSend = messageToSend;
            this.serviceCallFailureAction = serviceCallFailureAction;
            this.serviceId = RosServiceCallManager.GetUniqueServiceId();
            this.taskPauser = new TaskPauser();
        }

        public abstract void OnServiceCompletedSuccessfully(byte[] rawResponse);

        public void OnServiceCallFailed(Exception cause)
        {
            serviceCallFailureAction?.Invoke(cause);
            taskPauser.Resume(null);
        }
    }

    public class RosServiceCallInfo<RESPONSE> : RosServiceCallInfoBase where RESPONSE : Message
    {
        public Action<RESPONSE> responseAction;


        public RosServiceCallInfo(RosTopicState topicState, Message messageToSend,
            Action<Exception> serviceCallFailureAction, Action<RESPONSE> responseAction) : base(topicState,
            messageToSend, serviceCallFailureAction)
        {
            this.responseAction = responseAction;
            this.serviceCallFailureAction = serviceCallFailureAction;
        }

        public override void OnServiceCompletedSuccessfully(byte[] rawResponse)
        {
            topicState.OnMessageReceived(rawResponse);

            MessageDeserializer messageDeserializer = new MessageDeserializer();
            RESPONSE result = messageDeserializer.DeserializeMessage<RESPONSE>(rawResponse);
            responseAction.Invoke(result);

            taskPauser.Resume(result);
        }
    }
}
