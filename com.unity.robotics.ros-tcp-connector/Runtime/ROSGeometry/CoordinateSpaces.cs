using RosMessageTypes.Geometry;
using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector.ROSGeometry
{
    public interface ICoordinateSpace
    {
        Vector3 ConvertFromRUF(Vector3 v, bool geographic = false); // convert this vector from the Unity coordinate space into mine
        Vector3 ConvertToRUF(Vector3 v, bool geographic = false); // convert from my coordinate space into the Unity coordinate space

        Quaternion ConvertFromRUF(Quaternion q, bool geographic = false); // convert this quaternion from the Unity coordinate space into mine
        Quaternion ConvertToRUF(Quaternion q, bool geographic = false); // convert from my coordinate space into the Unity coordinate space


    }

    [Obsolete("CoordinateSpace has been renamed to ICoordinateSpace")]
    public interface CoordinateSpace : ICoordinateSpace
    {
    }

    //RUF is the Unity coordinate space, so no conversion needed
    public class RUF : ICoordinateSpace
    {
        public virtual Vector3 ConvertFromRUF(Vector3 v, bool geographic = false) => v;
        public virtual Vector3 ConvertToRUF(Vector3 v, bool geographic = false) => v;
        public virtual Quaternion ConvertFromRUF(Quaternion q, bool geographic = false) => q;
        public virtual Quaternion ConvertToRUF(Quaternion q, bool geographic = false) => q;
    }

    public class FLU : ICoordinateSpace
    {
        public virtual Vector3 ConvertFromRUF(Vector3 v, bool geographic = false) => new Vector3(v.z, -v.x, v.y);
        public virtual Vector3 ConvertToRUF(Vector3 v, bool geographic = false) => new Vector3(-v.y, v.z, v.x);
        public virtual Quaternion ConvertFromRUF(Quaternion q, bool geographic = false) => new Quaternion(q.z, -q.x, q.y, -q.w);
        public virtual Quaternion ConvertToRUF(Quaternion q, bool geographic = false) => new Quaternion(-q.y, q.z, q.x, -q.w);
    }

    public class ENU : FLU
    {

        public override Vector3 ConvertFromRUF(Vector3 v, bool geographic = false)
        {
            Vector3 result = base.ConvertFromRUF(v);
            if (geographic)
            {
                result = FromRUFApplyUnityZAxisDirection(base.ConvertFromRUF(v), GeometryCompass.GlobalUnityZAxisDirection);
            }
            return result;
        }

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

        public override Vector3 ConvertToRUF(Vector3 v, bool geographic = false)
        {
            if (geographic)
            {
                v = ToRUFApplyUnityZAxisDirection(v, GeometryCompass.GlobalUnityZAxisDirection);
            }
            return base.ConvertToRUF(v);
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

        public override Quaternion ConvertFromRUF(Quaternion q, bool geographic = false)
        {
            if (geographic)
            {
                switch (GeometryCompass.GlobalUnityZAxisDirection)
                {
                    case CardinalDirection.North:
                        q = GeometryCompass.k_NegativeNinetyYaw * q;
                        break;
                    case CardinalDirection.East:
                        //Nothing to do here.
                        break;
                    case CardinalDirection.South:
                        q = GeometryCompass.k_NinetyYaw * q;
                        break;
                    case CardinalDirection.West:
                        q = GeometryCompass.k_OneEightyYaw * q;
                        break;
                    default:
                        throw new NotSupportedException();
                }
            }
            return base.ConvertFromRUF(q);
        }

        public override Quaternion ConvertToRUF(Quaternion q, bool geographic = false)
        {
            if (geographic)
            {
                switch (GeometryCompass.GlobalUnityZAxisDirection)
                {
                    case CardinalDirection.North:
                        q = GeometryCompass.k_NinetyYaw * q;
                        break;
                    case CardinalDirection.East:
                        //Nothing to do here.
                        break;
                    case CardinalDirection.South:
                        q = GeometryCompass.k_NegativeNinetyYaw * q;
                        break;
                    case CardinalDirection.West:
                        q = GeometryCompass.k_OneEightyYaw * q;
                        break;
                    default:
                        throw new NotSupportedException();
                }
            }
            return base.ConvertToRUF(q);
        }
    }

    public class FRD : ICoordinateSpace
    {
        public virtual Vector3 ConvertFromRUF(Vector3 v, bool geographic = false) => new Vector3(v.z, v.x, -v.y);
        public virtual Vector3 ConvertToRUF(Vector3 v, bool geographic = false) => new Vector3(v.y, -v.z, v.x);
        public virtual Quaternion ConvertFromRUF(Quaternion q, bool geographic = false) => new Quaternion(q.z, q.x, -q.y, -q.w);
        public virtual Quaternion ConvertToRUF(Quaternion q, bool geographic = false) => new Quaternion(q.y, -q.z, q.x, -q.w);
    }

    public class NED : FRD
    {
        public override Vector3 ConvertFromRUF(Vector3 v, bool geographic = false)
        {
            Vector3 result = base.ConvertFromRUF(v);
            if (geographic)
            {
                result = FromRUFApplyUnityZAxisDirection(base.ConvertFromRUF(v), GeometryCompass.GlobalUnityZAxisDirection);
            }
            return result;
        }

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

        public override Vector3 ConvertToRUF(Vector3 v, bool geographic = false)
        {
            if (geographic)
            {
                v = ToRUFApplyUnityZAxisDirection(v, GeometryCompass.GlobalUnityZAxisDirection);
            }
            return base.ConvertToRUF(v);
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

        public override Quaternion ConvertFromRUF(Quaternion q, bool geographic = false)
        {
            if (geographic)
            {
                switch (GeometryCompass.GlobalUnityZAxisDirection)
                {
                    case CardinalDirection.North:
                        //Nothing to do here.
                        break;
                    case CardinalDirection.East:
                        q = GeometryCompass.k_NinetyYaw * q;
                        break;
                    case CardinalDirection.South:
                        q = GeometryCompass.k_OneEightyYaw * q;
                        break;
                    case CardinalDirection.West:
                        q = GeometryCompass.k_NegativeNinetyYaw * q;
                        break;
                    default:
                        throw new NotSupportedException();
                }
            }
            return base.ConvertFromRUF(q);
        }

        public override Quaternion ConvertToRUF(Quaternion q, bool geographic = false)
        {
            if (geographic)
            {
                switch (GeometryCompass.GlobalUnityZAxisDirection)
                {
                    case CardinalDirection.North:
                        //Nothing to do here.
                        break;
                    case CardinalDirection.East:
                        q = GeometryCompass.k_NegativeNinetyYaw * q;
                        break;
                    case CardinalDirection.South:
                        q = GeometryCompass.k_OneEightyYaw * q;
                        break;
                    case CardinalDirection.West:
                        q = GeometryCompass.k_NinetyYaw * q;
                        break;
                    default:
                        throw new NotSupportedException();
                }
            }
            return base.ConvertToRUF(q);
        }
    }


    public enum CoordinateSpaceSelection
    {
        RUF,
        FLU,
        FRD,
        NED,
        ENU
    }

    public static class CoordinateSpaceExtensions
    {
        public static Vector3<C> To<C>(this Vector3 self, bool geographic = false)
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
            return new Vector3<C>((float)self.x, (float)self.y, (float)self.z);
        }

        public static Vector3 From<C>(this PointMsg self) where C : ICoordinateSpace, new()
        {
            return new Vector3<C>((float)self.x, (float)self.y, (float)self.z).toUnity;
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
            return new Vector3<C>((float)self.x, (float)self.y, (float)self.z);
        }

        public static Vector3 From<C>(this Vector3Msg self) where C : ICoordinateSpace, new()
        {
            return new Vector3<C>((float)self.x, (float)self.y, (float)self.z).toUnity;
        }

        public static Quaternion<C> As<C>(this QuaternionMsg self) where C : ICoordinateSpace, new()
        {
            return new Quaternion<C>((float)self.x, (float)self.y, (float)self.z, (float)self.w);
        }

        public static Quaternion From<C>(this QuaternionMsg self) where C : ICoordinateSpace, new()
        {
            return new Quaternion<C>((float)self.x, (float)self.y, (float)self.z, (float)self.w).toUnity;
        }

        public static TransformMsg To<C>(this Transform transform) where C : ICoordinateSpace, new()
        {
            return new TransformMsg(new Vector3<C>(transform.position), new Quaternion<C>(transform.rotation));
        }

        public static TransformMsg ToLocal<C>(this Transform transform) where C : ICoordinateSpace, new()
        {
            return new TransformMsg(new Vector3<C>(transform.localPosition), new Quaternion<C>(transform.localRotation));
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
}
