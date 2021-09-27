using RosMessageTypes.Geometry;
using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector.ROSGeometry
{
    public interface ICoordinateSpace
    {
        Vector3 ConvertFromRUF(Vector3 v); // convert this vector from the Unity coordinate space into mine
        Vector3 ConvertToRUF(Vector3 v); // convert from my coordinate space into the Unity coordinate space

        Quaternion ConvertFromRUF(Quaternion q); // convert this quaternion from the Unity coordinate space into mine
        Quaternion ConvertToRUF(Quaternion q); // convert from my coordinate space into the Unity coordinate space
    }

    [Obsolete("CoordinateSpace has been renamed to ICoordinateSpace")]
    public interface CoordinateSpace : ICoordinateSpace
    {
    }

    //RUF is the Unity coordinate space, so no conversion needed
    public class RUF : ICoordinateSpace
    {
        public virtual Vector3 ConvertFromRUF(Vector3 v) => v;
        public virtual Vector3 ConvertToRUF(Vector3 v) => v;
        public virtual Quaternion ConvertFromRUF(Quaternion q) => q;
        public virtual Quaternion ConvertToRUF(Quaternion q) => q;
    }

    public class FLU : ICoordinateSpace
    {
        public virtual Vector3 ConvertFromRUF(Vector3 v) => new Vector3(v.z, -v.x, v.y);
        public virtual Vector3 ConvertToRUF(Vector3 v) => new Vector3(-v.y, v.z, v.x);
        public virtual Quaternion ConvertFromRUF(Quaternion q) => new Quaternion(q.z, -q.x, q.y, -q.w);
        public virtual Quaternion ConvertToRUF(Quaternion q) => new Quaternion(-q.y, q.z, q.x, -q.w);
    }

    public class ENU : FLU
    {
        public override Vector3 ConvertFromRUF(Vector3 v)
        {
            Vector3 result = base.ConvertFromRUF(v);
            result = FromRUFApplyUnityZAxisDirection(result, GeometryCompass.GlobalUnityZAxisDirection);
            return result;
        }

        public override Vector3 ConvertToRUF(Vector3 v)
        {
            v = ToRUFApplyUnityZAxisDirection(v, GeometryCompass.GlobalUnityZAxisDirection);
            return ConvertToRUF(v);
        }

        public override Quaternion ConvertFromRUF(Quaternion q)
        {
            q = FromRUFApplyUnityZAxisDirection(q, GeometryCompass.GlobalUnityZAxisDirection);
            return base.ConvertFromRUF(q);
        }

        public override Quaternion ConvertToRUF(Quaternion q)
        {
            Quaternion result = base.ConvertToRUF(q);
            result = ToRUFApplyUnityZAxisDirection(result, GeometryCompass.GlobalUnityZAxisDirection);
            return result;
        }

        #region UnityZAxisDirection

        public static Vector3 FromRUFApplyUnityZAxisDirection(Vector3 v, CardinalDirection unityZAxisDirection)
        {
            switch (unityZAxisDirection)
            {
                case CardinalDirection.North:
                    return new Vector3(-v.y, v.x, v.z);
                case CardinalDirection.East:
                    return new Vector3(v.x, v.y, v.z);
                case CardinalDirection.South:
                    return new Vector3(v.y, -v.x, v.z);
                case CardinalDirection.West:
                    return new Vector3(-v.x, -v.y, v.z);
                default:
                    throw new NotSupportedException();
            }
        }

        public static Vector3 ToRUFApplyUnityZAxisDirection(Vector3 v, CardinalDirection unityZAxisDirection)
        {
            switch (unityZAxisDirection)
            {
                case CardinalDirection.North:
                    return new Vector3(v.y, -v.x, v.z);
                case CardinalDirection.East:
                    return new Vector3(v.x, v.y, v.z);
                case CardinalDirection.South:
                    return new Vector3(-v.y, v.x, v.z);
                case CardinalDirection.West:
                    return new Vector3(-v.x, -v.y, v.z);
                default:
                    throw new NotSupportedException();
            }
        }

        public static Quaternion FromRUFApplyUnityZAxisDirection(Quaternion q, CardinalDirection unityZAxisDirection)
        {
            switch (unityZAxisDirection)
            {
                case CardinalDirection.North:
                    return GeometryCompass.k_NegativeNinetyYaw * q;
                case CardinalDirection.East:
                    //Nothing to do here.
                    return q;
                case CardinalDirection.South:
                    return GeometryCompass.k_NinetyYaw * q;
                case CardinalDirection.West:
                    return GeometryCompass.k_OneEightyYaw * q;
                default:
                    throw new NotSupportedException();
            }
        }

        public static Quaternion ToRUFApplyUnityZAxisDirection(Quaternion q, CardinalDirection unityZAxisDirection)
        {
            switch (unityZAxisDirection)
            {
                case CardinalDirection.North:
                    return GeometryCompass.k_NinetyYaw * q;
                case CardinalDirection.East:
                    //Nothing to do here.
                    return q;
                case CardinalDirection.South:
                    return GeometryCompass.k_NegativeNinetyYaw * q;
                case CardinalDirection.West:
                    return GeometryCompass.k_OneEightyYaw * q;
                default:
                    throw new NotSupportedException();
            }
        }

        #endregion
    }

    public class FRD : ICoordinateSpace
    {
        public virtual Vector3 ConvertFromRUF(Vector3 v) => new Vector3(v.z, v.x, -v.y);
        public virtual Vector3 ConvertToRUF(Vector3 v) => new Vector3(v.y, -v.z, v.x);
        public virtual Quaternion ConvertFromRUF(Quaternion q) => new Quaternion(q.z, q.x, -q.y, -q.w);
        public virtual Quaternion ConvertToRUF(Quaternion q) => new Quaternion(q.y, -q.z, q.x, -q.w);
    }

    public class NED : FRD
    {
        public override Vector3 ConvertFromRUF(Vector3 v)
        {
            Vector3 result = base.ConvertFromRUF(v);
            result = FromRUFApplyUnityZAxisDirection(result, GeometryCompass.GlobalUnityZAxisDirection);
            return result;
        }

        public override Vector3 ConvertToRUF(Vector3 v)
        {
            v = ToRUFApplyUnityZAxisDirection(v, GeometryCompass.GlobalUnityZAxisDirection);
            return base.ConvertToRUF(v);
        }

        public override Quaternion ConvertFromRUF(Quaternion q)
        {
            q = FromRUFApplyUnityZAxisDirection(q, GeometryCompass.GlobalUnityZAxisDirection);
            return base.ConvertFromRUF(q);
        }

        public override Quaternion ConvertToRUF(Quaternion q)
        {
            Quaternion result = base.ConvertToRUF(q);
            result = ToRUFApplyUnityZAxisDirection(result, GeometryCompass.GlobalUnityZAxisDirection);
            return result;
        }

        #region UnityZAxisDirection

        public static Vector3 FromRUFApplyUnityZAxisDirection(Vector3 v, CardinalDirection unityZAxisDirection)
        {
            switch (unityZAxisDirection)
            {
                case CardinalDirection.North:
                    return new Vector3(v.x, v.y, v.z);
                case CardinalDirection.East:
                    return new Vector3(-v.y, v.x, v.z);
                case CardinalDirection.South:
                    return new Vector3(-v.x, -v.y, v.z);
                case CardinalDirection.West:
                    return new Vector3(v.y, -v.x, v.z);
                default:
                    throw new NotSupportedException();
            }
        }

        public static Vector3 ToRUFApplyUnityZAxisDirection(Vector3 v, CardinalDirection unityZAxisDirection)
        {
            switch (unityZAxisDirection)
            {
                case CardinalDirection.North:
                    return new Vector3(v.x, v.y, v.z);
                case CardinalDirection.East:
                    return new Vector3(v.y, -v.x, v.z);
                case CardinalDirection.South:
                    return new Vector3(-v.x, -v.y, v.z);
                case CardinalDirection.West:
                    return new Vector3(-v.y, v.x, v.z);
                default:
                    throw new NotSupportedException();
            }
        }

        public static Quaternion FromRUFApplyUnityZAxisDirection(Quaternion q, CardinalDirection unityZAxisDirection)
        {
            switch (unityZAxisDirection)
            {
                case CardinalDirection.North:
                    //Nothing to do here.
                    return q;
                case CardinalDirection.East:
                    return GeometryCompass.k_NinetyYaw * q;
                case CardinalDirection.South:
                    return GeometryCompass.k_OneEightyYaw * q;
                case CardinalDirection.West:
                    return GeometryCompass.k_NegativeNinetyYaw * q;
                default:
                    throw new NotSupportedException();
            }
        }

        public static Quaternion ToRUFApplyUnityZAxisDirection(Quaternion q, CardinalDirection unityZAxisDirection)
        {
            switch (unityZAxisDirection)
            {
                case CardinalDirection.North:
                    //Nothing to do here.
                    return q;
                case CardinalDirection.East:
                    return GeometryCompass.k_NegativeNinetyYaw * q;
                case CardinalDirection.South:
                    return GeometryCompass.k_OneEightyYaw * q;
                case CardinalDirection.West:
                    return GeometryCompass.k_NinetyYaw * q;
                default:
                    throw new NotSupportedException();
            }
        }

        #endregion
    }


    public enum CoordinateSpaceSelection
    {
        RUF,
        FLU,
        ENU,
        FRD,
        NED,
    }

    public static class CoordinateSpaceExtensions
    {
        public static Vector3<C> To<C>(this Vector3 self)
            where C : ICoordinateSpace, new()
        {
            return new Vector3<C>(self);
        }

        public static Quaternion<C> To<C>(this Quaternion self)
            where C : ICoordinateSpace, new()
        {
            return new Quaternion<C>(self);
        }

        public static Vector3<C> As<C>(this PointMsg self) where C : ICoordinateSpace, new()
        {
            return new Vector3<C>((float) self.x, (float) self.y, (float) self.z);
        }

        public static Vector3 From<C>(this PointMsg self) where C : ICoordinateSpace, new()
        {
            return new Vector3<C>((float) self.x, (float) self.y, (float) self.z).toUnity;
        }

        public static Vector3<C> As<C>(this Point32Msg self) where C : ICoordinateSpace, new()
        {
            return new Vector3<C>(self.x, self.y, self.z);
        }

        public static Vector3 From<C>(this Point32Msg self) where C : ICoordinateSpace, new()
        {
            return new Vector3<C>(self.x, self.y, self.z).toUnity;
        }

        public static Vector3<C> As<C>(this Vector3Msg self) where C : ICoordinateSpace, new()
        {
            return new Vector3<C>((float) self.x, (float) self.y, (float) self.z);
        }

        public static Vector3 From<C>(this Vector3Msg self) where C : ICoordinateSpace, new()
        {
            return new Vector3<C>((float) self.x, (float) self.y, (float) self.z).toUnity;
        }

        public static Quaternion<C> As<C>(this QuaternionMsg self) where C : ICoordinateSpace, new()
        {
            return new Quaternion<C>((float) self.x, (float) self.y, (float) self.z, (float) self.w);
        }

        public static Quaternion From<C>(this QuaternionMsg self) where C : ICoordinateSpace, new()
        {
            return new Quaternion<C>((float) self.x, (float) self.y, (float) self.z, (float) self.w).toUnity;
        }

        public static TransformMsg To<C>(this Transform transform) where C : ICoordinateSpace, new()
        {
            return new TransformMsg(new Vector3<C>(transform.position), new Quaternion<C>(transform.rotation));
        }

        public static TransformMsg ToLocal<C>(this Transform transform) where C : ICoordinateSpace, new()
        {
            return new TransformMsg(new Vector3<C>(transform.localPosition),
                new Quaternion<C>(transform.localRotation));
        }

        public static Vector3 From(this PointMsg self, CoordinateSpaceSelection selection)
        {
            switch (selection)
            {
                case CoordinateSpaceSelection.RUF:
                    return self.From<RUF>();
                case CoordinateSpaceSelection.FLU:
                    return self.From<FLU>();
                case CoordinateSpaceSelection.ENU:
                    return self.From<ENU>();
                case CoordinateSpaceSelection.FRD:
                    return self.From<FRD>();
                case CoordinateSpaceSelection.NED:
                    return self.From<NED>();
                default:
                    Debug.LogError("Invalid coordinate space " + selection);
                    return self.From<RUF>();
            }
        }

        public static Vector3 From(this Point32Msg self, CoordinateSpaceSelection selection)
        {
            switch (selection)
            {
                case CoordinateSpaceSelection.RUF:
                    return self.From<RUF>();
                case CoordinateSpaceSelection.FLU:
                    return self.From<FLU>();
                case CoordinateSpaceSelection.ENU:
                    return self.From<ENU>();
                case CoordinateSpaceSelection.FRD:
                    return self.From<FRD>();
                case CoordinateSpaceSelection.NED:
                    return self.From<NED>();
                default:
                    Debug.LogError("Invalid coordinate space " + selection);
                    return self.From<RUF>();
            }
        }

        public static Vector3 From(this Vector3Msg self, CoordinateSpaceSelection selection)
        {
            switch (selection)
            {
                case CoordinateSpaceSelection.RUF:
                    return self.From<RUF>();
                case CoordinateSpaceSelection.FLU:
                    return self.From<FLU>();
                case CoordinateSpaceSelection.ENU:
                    return self.From<ENU>();
                case CoordinateSpaceSelection.FRD:
                    return self.From<FRD>();
                case CoordinateSpaceSelection.NED:
                    return self.From<NED>();
                default:
                    Debug.LogError("Invalid coordinate space " + selection);
                    return self.From<RUF>();
            }
        }

        public static Quaternion From(this QuaternionMsg self, CoordinateSpaceSelection selection)
        {
            switch (selection)
            {
                case CoordinateSpaceSelection.RUF:
                    return self.From<RUF>();
                case CoordinateSpaceSelection.FLU:
                    return self.From<FLU>();
                case CoordinateSpaceSelection.ENU:
                    return self.From<ENU>();
                case CoordinateSpaceSelection.FRD:
                    return self.From<FRD>();
                case CoordinateSpaceSelection.NED:
                    return self.From<NED>();
                default:
                    Debug.LogError("Invalid coordinate space " + selection);
                    return self.From<RUF>();
            }
        }
    }

    public enum RobotCoordinateSpacesSelection
    {
        ENUFLU,
        NEDFRD
    }

    public static class RobotCoordinateSpacesExtensions
    {
        public static ICoordinateSpace Local(this RobotCoordinateSpacesSelection rc) {

            switch (rc)
            {
                case RobotCoordinateSpacesSelection.ENUFLU:
                    return new FLU();
                case RobotCoordinateSpacesSelection.NEDFRD:
                    return new FRD();
                default:
                    Debug.LogError("Invalid coordinate space " + rc);
                    return new FLU();
            }
        }

        public static ICoordinateSpace Global(this RobotCoordinateSpacesSelection rc) {

            switch (rc)
            {
                case RobotCoordinateSpacesSelection.ENUFLU:
                    return new ENU();
                case RobotCoordinateSpacesSelection.NEDFRD:
                    return new NED();
                default:
                    Debug.LogError("Invalid coordinate space " + rc);
                    return new ENU();
            }
        }

        public static Vector3Msg ToLocal(this RobotCoordinateSpacesSelection rc, Vector3 vector3)
        {
            Vector3 convertedVector = rc.Local().ConvertFromRUF(vector3);
            return new Vector3Msg(convertedVector.x, convertedVector.y, convertedVector.z);
        }

        public static Vector3Msg ToGlobal(this RobotCoordinateSpacesSelection rc, Vector3 vector3)
        {
            Vector3 convertedVector = rc.Global().ConvertFromRUF(vector3);
            return new Vector3Msg(convertedVector.x, convertedVector.y, convertedVector.z);
        }

        public static QuaternionMsg ToLocal(this RobotCoordinateSpacesSelection rc, Quaternion quaternion)
        {
            Quaternion convertedQuaternion = rc.Local().ConvertFromRUF(quaternion);
            return new QuaternionMsg(convertedQuaternion.x, convertedQuaternion.y, convertedQuaternion.z, convertedQuaternion.w);
        }

        public static QuaternionMsg ToGlobal(this RobotCoordinateSpacesSelection rc, Quaternion quaternion)
        {
            Quaternion convertedQuaternion = rc.Global().ConvertFromRUF(quaternion);
            return new QuaternionMsg(convertedQuaternion.x, convertedQuaternion.y, convertedQuaternion.z, convertedQuaternion.w);
        }

        public static Vector3Msg FromLocal(this RobotCoordinateSpacesSelection rc, Vector3 vector3)
        {
            Vector3 convertedVector = rc.Local().ConvertToRUF(vector3);
            return new Vector3Msg(convertedVector.x, convertedVector.y, convertedVector.z);
        }

        public static Vector3Msg FromGlobal(this RobotCoordinateSpacesSelection rc, Vector3 vector3)
        {
            Vector3 convertedVector = rc.Global().ConvertToRUF(vector3);
            return new Vector3Msg(convertedVector.x, convertedVector.y, convertedVector.z);
        }

        public static QuaternionMsg FromLocal(this RobotCoordinateSpacesSelection rc, Quaternion quaternion)
        {
            Quaternion convertedQuaternion = rc.Local().ConvertToRUF(quaternion);
            return new QuaternionMsg(convertedQuaternion.x, convertedQuaternion.y, convertedQuaternion.z, convertedQuaternion.w);
        }

        public static QuaternionMsg FromGlobal(this RobotCoordinateSpacesSelection rc, Quaternion quaternion)
        {
            Quaternion convertedQuaternion = rc.Global().ConvertToRUF(quaternion);
            return new QuaternionMsg(convertedQuaternion.x, convertedQuaternion.y, convertedQuaternion.z, convertedQuaternion.w);
        }
    }

}
