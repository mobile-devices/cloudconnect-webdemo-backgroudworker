using CloudConnect.CouchBaseProvider;
using MD.CloudConnect;
using SharpRaven;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Timers;

namespace CloudConnect.BackgroundWorker
{
    public partial class NotificationWorker : ServiceBase
    {
        private bool _workInProgressForNotification = false;

        private Timer _notificationtimer;

        private FieldManager _fieldManager;
        private DeviceManager _deviceManager;

        private string _lastNotifId = String.Empty;

        public NotificationWorker()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            InternalLogger.WriteLog("Notification Worker v1.0");
            _fieldManager = FieldManager.Instance;
            _deviceManager = DeviceManager.Instance;

            _notificationtimer = new Timer(5000);
            _notificationtimer.Elapsed += _notificationtimer_Elapsed;
            InternalLogger.WriteLog("Start Notification Timer");
            _notificationtimer.Enabled = true;
        }

        protected override void OnStop()
        {
            InternalLogger.WriteLog("Stop Notification Timer");
            _notificationtimer.Enabled = false;
        }

        void _notificationtimer_Elapsed(object sender, ElapsedEventArgs e)
        {
            if (_workInProgressForNotification)
            {
                return;
            }
            _workInProgressForNotification = true;
            try
            {
                Stopwatch watch = new Stopwatch();
                watch.Start();
                int total = GenerateTrack();
                InternalLogger.WriteLog(String.Format("[Notif Worker] {0} tracks", total));
                InternalLogger.WriteLog(String.Format("[Notif Worker] Decode and store :{0} ms", watch.ElapsedMilliseconds));
            }
            catch (Exception ex)
            {
                Console.WriteLine("[Notif Worker] error" + ex.Message);
            }
            finally
            {
                _workInProgressForNotification = false;
            }
        }


        private Dictionary<string, CloudConnect.CouchBaseProvider.Field> BuildFields(Dictionary<string, MD.CloudConnect.Data.Field> fields, DateTime recordedAt)
        {
            Dictionary<string, CloudConnect.CouchBaseProvider.Field> result = new Dictionary<string, CloudConnect.CouchBaseProvider.Field>();
            foreach (KeyValuePair<string, MD.CloudConnect.Data.Field> item in fields)
            {
                result.Add(item.Key.ToLowerInvariant(), new CloudConnect.CouchBaseProvider.Field()
                    {
                        Key = item.Key,
                        B64Value = item.Value.b64_value,
                        RecordedAt = recordedAt
                    });
            }
            return result;
        }

        private bool CanBeGenerate(Track t)
        {
            Device d = _deviceManager.GetDevice(t.Imei);
            if (d.LastRecordedAt <= t.Recorded_at)
                return true;
            else
            {
                //if (String.IsNullOrEmpty(d.LastTrackId))
                //    return true;

                //if track already exist in database
                //remove from the list
                //if (String.IsNullOrEmpty(d.LastCloudId))
                //{
                //    Track lastTrack = CouchbaseManager.Instance.TrackRepository.Get(d.LastTrackId);
                //    d.LastCloudId = lastTrack.CloudId;
                //}
                //UInt64 currentCloudID = UInt64.Parse(t.CloudId);
                //UInt64 deviceCloudID = UInt64.Parse(d.LastCloudId);
                //if (deviceCloudID > currentCloudID)
                //    return false;
                //else
                //    return true;
                string key = CouchbaseManager.Instance.TrackRepository.BuildKey(t);
                Track lastTrack = CouchbaseManager.Instance.TrackRepository.Get(key);
                if (lastTrack == null)
                    return true;
                else return false;
            }
        }

        private int GenerateTrack()
        {
            int totalTrackGenerated = 0;

            Stopwatch watch = new Stopwatch();
            watch.Start();

            List<CloudConnect.CouchBaseProvider.Notification> data = CouchbaseManager.Instance.NotificationRepository.RequestNotificationCache("GLOBAL", 500, true, _lastNotifId);

            InternalLogger.WriteLog(String.Format("[Notif Worker]Load Notif :{0} ms", watch.ElapsedMilliseconds));
            watch.Restart();

            if (data.Count > 0)
            {
                if (String.IsNullOrEmpty(_lastNotifId))
                {
                    _lastNotifId = data.Last().Id;
                }
                else
                {
                    _lastNotifId = data.First().Id;
                    data.RemoveAt(0);
                }
                List<CloudConnect.CouchBaseProvider.Track> tracks = new List<Track>();
                List<string> cacheKey = new List<string>();
                List<MDData> result = null;
                foreach (CloudConnect.CouchBaseProvider.Notification dNotif in data)
                {
                    if (dNotif.Status == 0)
                    {
                        try
                        {
                            result = MD.CloudConnect.Notification.Instance.Decode(dNotif.Data);
                        }
                        catch
                        {
                            dNotif.Status = 2;
                        }

                        try
                        {
                            foreach (MDData decodedData in result)
                            {
                                if (decodedData.Meta.Event == "track")
                                {
                                    totalTrackGenerated += 1;
                                    ITracking t = decodedData.Tracking;

                                    CloudConnect.CouchBaseProvider.Track newTrack = new Track()
                                    {
                                        Fields = BuildFields(t.Fields, t.Recorded_at),
                                        Imei = t.Asset,
                                        Account = decodedData.Meta.Account,
                                        Latitude = t.Latitude,
                                        Longitude = t.Longitude,
                                        Received_at = t.Received_at,
                                        Recorded_at = t.Recorded_at,
                                        Status = 0,
                                        CloudId = t.Id_str,
                                        Index = t.Index,
                                        ConnectionId = t.ConnectionId.ToString()
                                    };

                                    //Cloud ID is unique, if we alreday know the id mean duplicate data
                                    if (!cacheKey.Contains(newTrack.CloudId) && CanBeGenerate(newTrack))
                                    {
                                        tracks.Add(newTrack);
                                        cacheKey.Add(newTrack.CloudId);

                                        if (tracks.Count >= 1000)
                                        {
                                            CouchbaseManager.Instance.TrackRepository.BulkUpsert(tracks);
                                            tracks.Clear();
                                            cacheKey.Clear();
                                        }
                                    }
                                }
                            }
                            //update notification status
                            dNotif.Status = 1;
                        }
                        catch (Exception ex)
                        {
                            InternalLogger.WriteLog(String.Format("[Notif Worker] Exception : {0}", ex.Message));
                        }
                    }
                }
                InternalLogger.WriteLog(String.Format("[Notif Worker]End Parse :{0} ms", watch.ElapsedMilliseconds));
                watch.Restart();
                if (tracks.Count > 0)
                    CouchbaseManager.Instance.TrackRepository.BulkUpsert(tracks);
                CouchbaseManager.Instance.NotificationRepository.BulkUpsert(data);
                InternalLogger.WriteLog(String.Format("[Notif Worker]Save Notif :{0} ms", watch.ElapsedMilliseconds));
                watch.Restart();
            }
            return totalTrackGenerated;
        }


    }
}
