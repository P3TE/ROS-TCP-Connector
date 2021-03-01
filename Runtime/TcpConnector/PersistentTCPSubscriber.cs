using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Threading;
using RosMessageGeneration;
using UnityEngine;

namespace Runtime.TcpConnector
{

    public class ReceivedMessageInfo
    {
        public string topicName;
        public byte[] readBuffer;

        public ReceivedMessageInfo(string topicName, byte[] readBuffer)
        {
            this.topicName = topicName;
            this.readBuffer = readBuffer;
        }
    }
    
    public class PersistentTCPSubscriber
    {

        private const int _NetworkTimeout = 2000; //ms.

        public static readonly byte[] _Preamble = {0xfe, 0xed, 0xf0, 0x0d};

        private Thread tcpHandlerThread = null;
        private bool threadRunning = false;
        
        private volatile TcpClient tcpClient = null;

        public ConcurrentQueue<ReceivedMessageInfo> receivedQueue = new ConcurrentQueue<ReceivedMessageInfo>();

        public bool ThreadRunning => threadRunning;

        public PersistentTCPSubscriber(TcpClient tcpClient)
        {
            this.tcpClient = tcpClient;
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

        private void TcpThreadHandle(object obj)
        {

            NetworkStream tcpStream = tcpClient.GetStream();

            try
            {
                while (threadRunning)
                {

                    bool success =
                        ROSConnection.ReadMessageData(tcpStream, out string topicName, out byte[] messageBytes);
                    if (!success)
                    {
                        break;
                    }
                    receivedQueue.Enqueue(new ReceivedMessageInfo(topicName, messageBytes));
                }
            }
            catch(Exception e)
            {
                if (ROSConnection.ReportServerErrors)
                {
                    Debug.LogError("Connection failed.");
                    Debug.LogException(e);
                }
            }

            Shutdown();
        }

        private void CloseClientConnection()
        {
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
        }

        public void Shutdown()
        {
            if (!threadRunning)
            {
                //Something else has called the shutdown.
                return;
            }
            threadRunning = false;

            CloseClientConnection();
            
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