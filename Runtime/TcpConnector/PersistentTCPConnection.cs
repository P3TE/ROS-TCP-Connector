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

        private object messageSendLock = new object();
        private string rosTopicName;
        private Message messageToSend;
        private ManualResetEvent newSendDataReadyEvent = new ManualResetEvent(false);

        public PersistentTCPConnection(string hostName, int hostPort)
        {
            this.hostName = hostName;
            this.hostPort = hostPort;
            StartThread();
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
            
            while (threadRunning)
            {
                
                try
                {
                    tcpClient = new TcpClient();
                    tcpClient.Connect(hostName, hostPort);
                    NetworkStream networkStream = tcpClient.GetStream();
                    networkStream.ReadTimeout = _NetworkTimeout;
                    while (threadRunning)
                    {
                        //Wait for more data to send.
                        newSendDataReadyEvent.WaitOne();
                        if (!threadRunning)
                        {
                            //The Thread is shutting down, exit the loop.
                            break;
                        }

                        //Grab the latest data.
                        string topicName;
                        Message message;
                        lock (messageSendLock)
                        {
                            topicName = this.rosTopicName;
                            message = this.messageToSend;
                            newSendDataReadyEvent.Reset();
                        }
                        
                        //Send the message off.
                        WriteDataStaggered(networkStream, topicName, message);
                    }
                }
                catch (Exception e)
                {
                    //Debug.LogWarning($"Connection failed: {e.Message}");
                    
                    if (threadRunning)
                    {
                        Thread.Sleep(100);
                    }
                }
                
            }
            
            Shutdown();
        }
        
        private void WriteDataStaggered(NetworkStream networkStream, string rosTopicName, Message message)
        {
            
            //Write a preamble as a simple synchronisation check. 
            networkStream.Write(_Preamble, 0, _Preamble.Length);
            
            byte[] topicName = message.SerializeString(rosTopicName);
            List<byte[]> segments = message.SerializationStatements();
            int messageLength = 0;
            for (int i = 0; i < segments.Count; i++)
            {
                messageLength += segments[i].Length;
            }
            byte[] fullMessageSizeBytes = BitConverter.GetBytes(messageLength);

            networkStream.Write(topicName, 0, topicName.Length);
            networkStream.Write(fullMessageSizeBytes, 0, fullMessageSizeBytes.Length);
            for (int i = 0; i < segments.Count; i++)
            {
                byte[] segmentData = segments[i];
                networkStream.Write(segmentData, 0, segmentData.Length);
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
                    Debug.Log("Closing TCP Connection...");
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