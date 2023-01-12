using System.Collections.Generic;
using System.IO;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector.RosService
{
    public class OutgoingServiceCallSender : OutgoingMessageSender
    {

        private RosServiceCallInfoBase serviceCallInfo;

        private bool dataQueueCleared = false;

        public OutgoingServiceCallSender(RosServiceCallInfoBase serviceCallInfo)
        {
            this.serviceCallInfo = serviceCallInfo;
        }

        public override SendToState SendInternal(MessageSerializer m_MessageSerializer, Stream stream)
        {
            if (dataQueueCleared)
            {
                return SendToState.NoMessageToSendError;
            }

            m_MessageSerializer.Clear();

            //Send the sys command.
            ROSConnection.PopulateSysCommand(m_MessageSerializer, SysCommand.k_SysCommand_ServiceRequest, new SysCommand_Service { srv_id = serviceCallInfo.serviceId });
            m_MessageSerializer.SendTo(stream);

            //Send the message.
            TopicMessageSender.SendMessageWithStreamTo(
                serviceCallInfo.topicState.Topic, m_MessageSerializer, stream, serviceCallInfo.messageToSend);

            return SendToState.Normal;
        }

        public override void ClearAllQueuedData()
        {
            dataQueueCleared = true;
            serviceCallInfo.OnServiceCallFailed(new RosConnectionException("Service cancelled as no ROS connection exists."));
            bool removalSuccessful = RosServiceCallManager.TryRemoveWaitingService(serviceCallInfo.serviceId,
                out RosServiceCallInfoBase _);
            if (!removalSuccessful)
            {
                Debug.LogWarning("Logic error, clear called on a service that didn't exist within the service call manager.");
            }
        }
    }
}
