using MD.CloudConnect.CouchBaseProvider;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudConnect.BackgroundWorker
{
    public class DeviceManager
    {
        Dictionary<string, Device> _cache = new Dictionary<string, Device>();
        private DateTime _lastCacheUpdated = DateTime.MinValue;

        #region singleton
        protected static readonly DeviceManager _instance = new DeviceManager();
        public static DeviceManager Instance
        {
            get
            {
                lock (_instance)
                {
                    return _instance;
                }
            }
        }

        static DeviceManager()
        {

        }
        #endregion

        public Device GetDevice(string imei)
        {
            if (_cache.ContainsKey(imei))
                return _cache[imei];
            else
            {
                Device newDevice = CouchbaseManager.Instance.DeviceRepository.Get(imei);
                if (newDevice == null)
                {
                    newDevice = new Device()
                    {
                        CreatedAt = DateTime.UtcNow,
                        Fields = new Dictionary<string, Field>(),
                        Imei = imei,
                        UpdatedAt = DateTime.UtcNow
                    };
                }
                _cache.Add(newDevice.Imei, newDevice);
                return newDevice;
            }
        }

        public void FlushModification(List<string> devices)
        {
            if (devices.Count > 0)
            {
                List<Device> mustBeUpdate = new List<Device>(_cache.Values.Where(x => devices.Contains(x.Imei)));
                CouchbaseManager.Instance.DeviceRepository.BulkUpsert(mustBeUpdate);
            }
        }
    }
}
