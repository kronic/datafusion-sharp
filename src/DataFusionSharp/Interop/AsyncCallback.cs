using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
internal delegate void AsyncCallback(IntPtr result, IntPtr error, ulong userData);