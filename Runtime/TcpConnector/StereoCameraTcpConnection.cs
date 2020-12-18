using RosMessageGeneration;

namespace Runtime.TcpConnector
{
    public class StereoCameraTcpConnection : PersistentTCPConnection {
        
        public StereoCameraTcpConnection(string hostName, int hostPort) : base(hostName, hostPort)
        {
        }
        
        public StereoCameraTcpConnection() : base(ROSConnection.Instance.hostName, ROSConnection.Instance.hostPort)
        {
        }
        
        private string rosTopicCamLeftRaw;
        private string rosTopicCamLeftInfo;
        private string rosTopicCamRightRaw;
        private string rosTopicCamRightInfo;
        
        private Message rosMessageCamLeftRaw;
        private Message rosMessageCamLeftInfo;
        private Message rosMessageCamRightRaw;
        private Message rosMessageCamRightInfo;
        
        public void Send(string rosTopicCamLeftRaw,
            string rosTopicCamLeftInfo,
            string rosTopicCamRightRaw,
            string rosTopicCamRightInfo,
            Message rosMessageCamLeftRaw,
            Message rosMessageCamLeftInfo,
            Message rosMessageCamRightRaw,
            Message rosMessageCamRightInfo)
        {
            lock (messageSendLock)
            {
                this.rosTopicCamLeftRaw = rosTopicCamLeftRaw;
                this.rosTopicCamLeftInfo = rosTopicCamLeftInfo;
                this.rosTopicCamRightRaw = rosTopicCamRightRaw;
                this.rosTopicCamRightInfo = rosTopicCamRightInfo;
                this.rosMessageCamLeftRaw = rosMessageCamLeftRaw;
                this.rosMessageCamLeftInfo = rosMessageCamLeftInfo;
                this.rosMessageCamRightRaw = rosMessageCamRightRaw;
                this.rosMessageCamRightInfo = rosMessageCamRightInfo;
                newSendDataReadyEvent.Set();
            }
        }

        protected override void PrepareDataToSend()
        {
            AppendBuildDataToTransmitBuffer(rosTopicCamLeftRaw, rosMessageCamLeftRaw);
            AppendBuildDataToTransmitBuffer(rosTopicCamLeftInfo, rosMessageCamLeftInfo);
            AppendBuildDataToTransmitBuffer(rosTopicCamRightRaw, rosMessageCamRightRaw);
            AppendBuildDataToTransmitBuffer(rosTopicCamRightInfo, rosMessageCamRightInfo);
        }
    }
}