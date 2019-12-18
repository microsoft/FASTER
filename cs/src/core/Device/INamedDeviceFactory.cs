using System.Collections;
using System.Collections.Generic;

namespace FASTER.core
{
    /// <summary>
    /// 
    /// </summary>
    // TODO(Tianyu): Do we need to have a generic argument for the underlying device type?
    public interface INamedDeviceFactory
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="container"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        IDevice GetOrCreateFromName(string container, string name);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="container"></param>
        /// <returns></returns>
        IEnumerable<IDevice> ListDevicesNewestToOldest(string container);
    }
}