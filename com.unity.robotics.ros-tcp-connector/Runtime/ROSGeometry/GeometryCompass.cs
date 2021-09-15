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

        public static Vector3<ENU> ToENU(Vector3 v, CardinalDirection unityZAxisDirection)
        {
            switch (unityZAxisDirection)
            {
                case CardinalDirection.North:
                    return new Vector3<ENU>(v.x, v.z, v.y);
                case CardinalDirection.East:
                    return new Vector3<ENU>(v.z, -v.x, v.y);
                case CardinalDirection.South:
                    return new Vector3<ENU>(-v.x, -v.z, v.y);
                case CardinalDirection.West:
                    return new Vector3<ENU>(-v.z, v.x, v.y);
                default:
                    throw new NotSupportedException();
            }
        }

        public Vector3<ENU> ToENU(Vector3 v)
        {
            switch (m_UnityZAxisDirection)
            {
                case CardinalDirection.North:
                    return new Vector3<ENU>(v.x, v.z, v.y);
                case CardinalDirection.East:
                    return new Vector3<ENU>(v.z, -v.x, v.y);
                case CardinalDirection.South:
                    return new Vector3<ENU>(-v.x, -v.z, v.y);
                case CardinalDirection.West:
                    return new Vector3<ENU>(-v.z, v.x, v.y);
                default:
                    throw new NotSupportedException();
            }
        }

        public Quaternion<ENU> ToENU(Quaternion q)
        {
            var r = Quaternion.Euler(0, 90 * ((int)m_UnityZAxisDirection - 1), 0) * q;
            return r.To<ENU>();
            //return new Quaternion<ENU>(r.x, r.z, r.y, -r.w);
        }

        public Vector3 FromENU(Vector3<ENU> v)
        {
            switch (m_UnityZAxisDirection)
            {
                case CardinalDirection.North:
                    return new Vector3(v.x, v.z, v.y);
                case CardinalDirection.East:
                    return new Vector3(-v.y, v.z, v.x);
                case CardinalDirection.South:
                    return new Vector3(-v.x, v.z, -v.y);
                case CardinalDirection.West:
                    return new Vector3(v.y, v.z, -v.x);
                default:
                    throw new NotSupportedException();
            }
        }

        public Quaternion FromENU(Quaternion<ENU> q)
        {
            var inverseRotationOffset = Quaternion.Euler(0, -90 * ((int)m_UnityZAxisDirection - 1), 0);
            return new Quaternion(q.x, q.z, q.y, -q.w) * inverseRotationOffset;
        }

        public Vector3<NED> ToNED(Vector3 v)
        {
            switch (m_UnityZAxisDirection)
            {
                case CardinalDirection.North:
                    return new Vector3<NED>(v.z, v.x, -v.y);
                case CardinalDirection.East:
                    return new Vector3<NED>(-v.x, v.z, -v.y);
                case CardinalDirection.South:
                    return new Vector3<NED>(-v.z, -v.x, -v.y);
                case CardinalDirection.West:
                    return new Vector3<NED>(v.x, -v.z, -v.y);
                default:
                    throw new NotSupportedException();
            }
        }

        public Quaternion<NED> ToNED(Quaternion q)
        {
            var r = Quaternion.Euler(0, 90 * (int)m_UnityZAxisDirection, 0) * q;
            return new Quaternion<NED>(r.z, r.x, -r.y, -r.w);
        }

        public Vector3 FromNED(Vector3<NED> v)
        {
            switch (m_UnityZAxisDirection)
            {
                case CardinalDirection.North:
                    return new Vector3(v.y, -v.z, v.x);
                case CardinalDirection.East:
                    return new Vector3(-v.x, -v.z, v.y);
                case CardinalDirection.South:
                    return new Vector3(-v.y, -v.z, -v.x);
                case CardinalDirection.West:
                    return new Vector3(v.x, -v.z, -v.y);
                default:
                    throw new NotSupportedException();
            }
        }

        public Quaternion FromNED(Quaternion<NED> q)
        {
            var inverseRotationOffset = Quaternion.Euler(0, -90 * (int)m_UnityZAxisDirection, 0);
            return new Quaternion(q.y, -q.z, q.x, -q.w) * inverseRotationOffset;
        }

        #region NiceAdditions

        private static GeometryCompass _instance = null;

        public static GeometryCompass Instance
        {
            get
            {
                if (_instance == null)
                {
                    GeometryCompass[] sceneCompasses = FindObjectsOfType<GeometryCompass>();
                    if (sceneCompasses.Length == 0)
                    {
                        throw new Exception("No GeometryCompass in scene, please add one.");
                    }
                    if (sceneCompasses.Length > 1)
                    {
                        throw new Exception("Multiple instances of GeometryCompass in scene, please only have one.");
                    }

                    _instance = sceneCompasses[0];
                }

                return _instance;
            }
        }

        public Vector3 GetWorldXZDirection(CardinalDirection desiredDirection)
        {
            float[] matrix2X2;
            float x;
            float z;
            switch (desiredDirection)
            {
                case CardinalDirection.North:
                    x = 0;
                    z = 1;
                    break;
                case CardinalDirection.East:
                    x = 1;
                    z = 0;
                    break;
                case CardinalDirection.South:
                    x = 0;
                    z = -1;
                    break;
                case CardinalDirection.West:
                    x = -1;
                    z = 0;
                    break;
                default:
                    throw new Exception($"Unsupported CardinalDirection: {desiredDirection}");
            }


            switch (UnityZAxisDirection)
            {
                case CardinalDirection.North:
                    matrix2X2 = new float[] {1, 0, 0, 1};
                    break;
                case CardinalDirection.East:
                    matrix2X2 = new float[] {0, -1, 1, 0};
                    break;
                case CardinalDirection.South:
                    matrix2X2 = new float[] {-1, 0, 0, -1};
                    break;
                case CardinalDirection.West:
                    matrix2X2 = new float[] {0, 1, -1, 0};
                    break;
                default:
                    throw new Exception($"Unsupported CardinalDirection: {desiredDirection}");
            }

            return new Vector3(
                (matrix2X2[0] * x + matrix2X2[1] * z),
                0,
                (matrix2X2[2] * x + matrix2X2[3] * z)
                );
        }

        #endregion
    }
}
