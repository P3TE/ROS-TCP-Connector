using System;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector.ROSGeometry
{
    public enum CardinalDirection
    {
        North = 0,
        East = 1,
        South = 2,
        West = 3,
    }

    public class GeometryCompass : MonoBehaviour
    {
        [SerializeField]
        CardinalDirection m_UnityZAxisDirection;
        public CardinalDirection UnityZAxisDirection
        {
            get => m_UnityZAxisDirection;
            set => m_UnityZAxisDirection = value;
        }

        public static CardinalDirection GlobalUnityZAxisDirection
        {
            get
            {
                if (Instance == null)
                {
                    return CardinalDirection.North;
                }
                return Instance.UnityZAxisDirection;
            }
        }

        public static Quaternion k_NinetyYaw = Quaternion.Euler(0, 90, 0);
        public static Quaternion k_OneEightyYaw = Quaternion.Euler(0, 180, 0);
        public static Quaternion k_NegativeNinetyYaw = Quaternion.Euler(0, -90, 0);

        #region NiceAdditions

        private static GeometryCompass _instance = null;
        private static bool _instanceSearched = false;

        public static GeometryCompass Instance
        {
            get
            {
                if (_instance == null && !_instanceSearched)
                {
                    _instanceSearched = true;
                    GeometryCompass[] sceneCompasses = FindObjectsOfType<GeometryCompass>();
                    if (sceneCompasses.Length == 0)
                    {
                        Debug.LogWarning("No GeometryCompass in scene, please add one.");
                    }
                    else
                    {
                        if (sceneCompasses.Length > 1)
                        {
                            Debug.LogWarning("Multiple instances of GeometryCompass in scene, please only have one.");
                        }
                        _instance = sceneCompasses[0];
                    }
                }
                return _instance;
            }
        }

        public static Vector3 GetWorldXZDirection(CardinalDirection desiredDirection)
        {

            switch (GlobalUnityZAxisDirection)
            {
                case CardinalDirection.North:
                    switch (desiredDirection)
                    {
                        case CardinalDirection.North:
                            return new Vector3(0, 0, 1);
                        case CardinalDirection.East:
                            return new Vector3(1, 0, 0);
                        case CardinalDirection.South:
                            return new Vector3(0, 0, -1);
                        case CardinalDirection.West:
                            return new Vector3(-1, 0, 0);
                        default:
                            throw new Exception($"Unsupported CardinalDirection: {desiredDirection}");
                    }
                case CardinalDirection.East:
                    switch (desiredDirection)
                    {
                        case CardinalDirection.North:
                            return new Vector3(-1, 0, 0);
                        case CardinalDirection.East:
                            return new Vector3(0, 0, 1);
                        case CardinalDirection.South:
                            return new Vector3(1, 0, 0);
                        case CardinalDirection.West:
                            return new Vector3(0, 0, -1);
                        default:
                            throw new Exception($"Unsupported CardinalDirection: {desiredDirection}");
                    }
                case CardinalDirection.South:
                    switch (desiredDirection)
                    {
                        case CardinalDirection.North:
                            return new Vector3(0, 0, -1);
                        case CardinalDirection.East:
                            return new Vector3(-1, 0, 0);
                        case CardinalDirection.South:
                            return new Vector3(0, 0, 1);
                        case CardinalDirection.West:
                            return new Vector3(1, 0, 0);
                        default:
                            throw new Exception($"Unsupported CardinalDirection: {desiredDirection}");
                    }
                case CardinalDirection.West:
                    switch (desiredDirection)
                    {
                        case CardinalDirection.North:
                            return new Vector3(1, 0, 0);
                        case CardinalDirection.East:
                            return new Vector3(0, 0, -1);
                        case CardinalDirection.South:
                            return new Vector3(-1, 0, 0);
                        case CardinalDirection.West:
                            return new Vector3(0, 0, 1);
                        default:
                            throw new Exception($"Unsupported CardinalDirection: {desiredDirection}");
                    }
                default:
                    throw new Exception($"Unsupported CardinalDirection: {GlobalUnityZAxisDirection}");
            }
        }

        public static float GetUnityYawDegrees(CardinalDirection desiredDirection)
        {
            switch (GlobalUnityZAxisDirection)
            {
                case CardinalDirection.North:
                    switch (desiredDirection)
                    {
                        case CardinalDirection.North:
                            return 0.0f;
                        case CardinalDirection.East:
                            return 90.0f;
                        case CardinalDirection.South:
                            return 180.0f;
                        case CardinalDirection.West:
                            return -90.0f;
                        default:
                            throw new Exception($"Unsupported CardinalDirection: {desiredDirection}");
                    }
                case CardinalDirection.East:
                    switch (desiredDirection)
                    {
                        case CardinalDirection.North:
                            return -90.0f;
                        case CardinalDirection.East:
                            return 0.0f;
                        case CardinalDirection.South:
                            return 90.0f;
                        case CardinalDirection.West:
                            return 180.0f;
                        default:
                            throw new Exception($"Unsupported CardinalDirection: {desiredDirection}");
                    }
                case CardinalDirection.South:
                    switch (desiredDirection)
                    {
                        case CardinalDirection.North:
                            return 180.0f;
                        case CardinalDirection.East:
                            return -90.0f;
                        case CardinalDirection.South:
                            return 0.0f;
                        case CardinalDirection.West:
                            return 90.0f;
                        default:
                            throw new Exception($"Unsupported CardinalDirection: {desiredDirection}");
                    }
                case CardinalDirection.West:
                    switch (desiredDirection)
                    {
                        case CardinalDirection.North:
                            return 90.0f;
                        case CardinalDirection.East:
                            return 180.0f;
                        case CardinalDirection.South:
                            return -90.0f;
                        case CardinalDirection.West:
                            return 0.0f;
                        default:
                            throw new Exception($"Unsupported CardinalDirection: {desiredDirection}");
                    }
                default:
                    throw new Exception($"Unsupported CardinalDirection: {GlobalUnityZAxisDirection}");
            }
        }

        #endregion
    }
}
