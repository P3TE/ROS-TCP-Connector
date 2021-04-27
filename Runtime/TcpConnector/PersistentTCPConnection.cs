using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using RosMessageGeneration;
using UnityEngine;

namespace Runtime.TcpConnector
{
    public class PersistentTCPConnection
    {

        private const int _NetworkTimeout = 2000; //ms.

        public static readonly byte[] _Preamble = {0xfe, 0xed, 0xf0, 0x0d};

        private string hostName = "192.168.1.1";
        private int hostPort = 10000;

        private Thread tcpHandlerThread = null;
        private bool threadRunning = false;

        private volatile TcpClient tcpClient = null;

        protected object messageSendLock = new object();
        private string rosTopicName;
        private Message messageToSend;
        protected ManualResetEvent newSendDataReadyEvent = new ManualResetEvent(false);

        private byte[] transmitBuffer = new byte[0];
        private int transmitBufferOffset = 0;

        public PersistentTCPConnection(string hostName, int hostPort)
        {
            this.hostName = hostName;
            this.hostPort = hostPort;
            StartThread();
            ROSConnection.Instance.RegisterPersistentPublisher(this);
        }

        private void StartThread()
        {
            if (threadRunning)
            {
                throw new Exception("Thread is already running!");
            }

            threadRunning = true;
            tcpHandlerThread = new Thread(TcpThreadHandle);
            tcpHandlerThread.IsBackground = true;
            tcpHandlerThread.Start();

        }

        public void Send(string rosTopicName, Message messageToSend)
        {
            lock (messageSendLock)
            {
                this.rosTopicName = rosTopicName;
                this.messageToSend = messageToSend;
                newSendDataReadyEvent.Set();
            }
        }

        private void TcpThreadHandle(object obj)
        {

            bool createNewTcpClient = true;
            NetworkStream networkStream = null;

            while (threadRunning)
            {

                try
                {
                    
                    while (threadRunning)
                    {
                        if (!ROSConnection.Instance.UnityServerReady)
                        {
                            //Wait until the server is ready...
                            Thread.Sleep(10);
                            continue;
                        }
                        //Wait for more data to send.
                        newSendDataReadyEvent.WaitOne();
                        if (!threadRunning)
                        {
                            //The Thread is shutting down, exit the loop.
                            break;
                        }

                        lock (messageSendLock)
                        {
                            //Grab the latest data.
                            transmitBufferOffset = 0;
                            PrepareDataToSend();
                            newSendDataReadyEvent.Reset();
                        }

                        if (createNewTcpClient)
                        {
                            createNewTcpClient = false;
                            tcpClient = new TcpClient();
                            tcpClient.Connect(hostName, hostPort);
                            networkStream = tcpClient.GetStream();
                            networkStream.ReadTimeout = _NetworkTimeout;
                        }
                        

                        //Send the message off.
                        networkStream.Write(transmitBuffer, 0, transmitBufferOffset);
                        networkStream.Flush();
                        
                    }
                }
                catch (Exception e)
                {

                    createNewTcpClient = true;
                    if (tcpClient != null)
                    {
                        try
                        {
                            if (tcpClient.Connected)
                            {
                                tcpClient.Close();
                            }
                        }
                        catch
                        {
                            //Ignored...
                        }
                    }

                    if (transmitBuffer.Length == int.MaxValue)
                    {
                        //Always false to stop the compiler warning the e is an unused variable.
                        Debug.LogException(e);
                    }

                    if (threadRunning)
                    {
                        //Debug.LogWarning($"Connection failed for topic '{rosTopicName}': {e.Message}");
                        ROSConnection.Instance.RequestCheckConnection();
                        Thread.Sleep(100);
                    }
                }

            }

            Shutdown();
        }

        protected virtual void PrepareDataToSend()
        {
            AppendBuildDataToTransmitBuffer(rosTopicName, messageToSend);
        }

        protected void AppendBuildDataToTransmitBuffer(string topicName, Message message)
        {
            
            byte[] topicNameData = message.SerializeString(topicName);
            List<byte[]> segments = message.SerializationStatements();
            int messageLength = 0;
            for (int i = 0; i < segments.Count; i++)
            {
                messageLength += segments[i].Length;
            }

            byte[] fullMessageSizeBytes = BitConverter.GetBytes(messageLength);

            int requiredAdditionalBufferSize = _Preamble.Length + topicNameData.Length + fullMessageSizeBytes.Length + messageLength;
            EnsureTransmitBufferSize(transmitBufferOffset + requiredAdditionalBufferSize);

            System.Buffer.BlockCopy(_Preamble, 0, transmitBuffer, transmitBufferOffset, _Preamble.Length);
            transmitBufferOffset += _Preamble.Length;
            System.Buffer.BlockCopy(topicNameData, 0, transmitBuffer, transmitBufferOffset, topicNameData.Length);
            transmitBufferOffset += topicNameData.Length;
            System.Buffer.BlockCopy(fullMessageSizeBytes, 0, transmitBuffer, transmitBufferOffset, fullMessageSizeBytes.Length);
            transmitBufferOffset += fullMessageSizeBytes.Length;
            for (int i = 0; i < segments.Count; i++)
            {
                byte[] segment = segments[i];
                int segmentLength = segment.Length;
                System.Buffer.BlockCopy(segment, 0, transmitBuffer, transmitBufferOffset, segmentLength);
                transmitBufferOffset += segmentLength;
            }
        }

        private void EnsureTransmitBufferSize(int requiredSize)
        {
            if (transmitBuffer.Length < requiredSize)
            {
                //Increase the size.
                int newSize = Mathf.NextPowerOfTwo(requiredSize);
                byte[] newBuffer = new byte[newSize];
                //Copy the old data across.
                System.Buffer.BlockCopy(transmitBuffer, 0, newBuffer, 0, transmitBuffer.Length);
                transmitBuffer = newBuffer;
            }
        }

        public void Shutdown()
        {
            if (!threadRunning)
            {
                //Something else has called the shutdown.
                return;
            }
            threadRunning = false;

            //Release the other thread from it's hold.
            newSendDataReadyEvent.Set();
            
            //Try and close the tcp client.
            try
            {
                if (tcpClient != null)
                {
                    if (ROSConnection.ReportServerErrors)
                    {
                        Debug.Log("Closing TCP Connection...");
                    }
                    tcpClient.Close();
                }
            }
            catch (Exception)
            {
                //Ignored.
            } 
            
            if (tcpHandlerThread != null && tcpHandlerThread.IsAlive && Thread.CurrentThread != tcpHandlerThread)
            {
                bool threadShutdown = tcpHandlerThread.Join(1000);
                if (!threadShutdown)
                {
                    Debug.LogWarning("Failed to shutdown tcp thread!");
                }
            }
        }


    }
}