using MD.CloudConnect.CouchBaseProvider;
using MD.CloudConnect;
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

        private bool _workInProgressForTrack = false;
        private Timer _tracktimer;

        private List<string> _devicesRebuild = new List<string>();

        public NotificationWorker()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            InternalLogger.WriteLog("Notification Worker v1.0 & Track Rebuilder Worker v1.0");
            _fieldManager = FieldManager.Instance;
            _deviceManager = DeviceManager.Instance;

            _notificationtimer = new Timer(1000);
            _notificationtimer.Elapsed += _notificationtimer_Elapsed;
            InternalLogger.WriteLog("Start Notification Timer");
            _notificationtimer.Enabled = true;

            _tracktimer = new Timer(1000);
            _tracktimer.Elapsed += _tracktimer_Elapsed;
            InternalLogger.WriteLog("Start Track Timer");
            _tracktimer.Enabled = true;
        }

        protected override void OnStop()
        {
            InternalLogger.WriteLog("Stop Timer");
            _notificationtimer.Enabled = false;
            _tracktimer.Enabled = false;
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
                if (total > 0)
                {
                    InternalLogger.WriteLog(String.Format("[Notif Worker] {0} tracks", total));
                    InternalLogger.WriteLog(String.Format("[Notif Worker] Decode and store :{0} ms", watch.ElapsedMilliseconds));
                }
            }
            catch (Exception ex)
            {
                /// InternalLogger.WriteLog(CouchbaseManager.Instance.DataListRepository.FlushLog());
                InternalLogger.WriteLog("[Notif Worker] error" + ex.Message);
                InternalLogger.WriteLog("[Notif Worker] stacktrace" + ex.ToString());
            }
            finally
            {
                _workInProgressForNotification = false;
            }
        }


        private Dictionary<string, MD.CloudConnect.CouchBaseProvider.Field> BuildFields(Dictionary<string, MD.CloudConnect.Data.Field> fields, DateTime recordedAt)
        {
            Dictionary<string, MD.CloudConnect.CouchBaseProvider.Field> result = new Dictionary<string, MD.CloudConnect.CouchBaseProvider.Field>();
            foreach (KeyValuePair<string, MD.CloudConnect.Data.Field> item in fields)
            {
                result.Add(item.Key.ToLowerInvariant(), new MD.CloudConnect.CouchBaseProvider.Field()
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
            //Device d = _deviceManager.GetDevice(t.Imei);
            //if (d.LastRecordedAt <= t.Recorded_at)
            //    return true;
            //else
            //{
            //    string key = CouchbaseManager.Instance.TrackRepository.BuildKey(t);
            //    return !CouchbaseManager.Instance.TrackRepository.KeyExist(key);
            //}
            return true;
        }

        private int GenerateTrack()
        {
            int totalTrackGenerated = 0;

            Stopwatch watch = new Stopwatch();
            watch.Start();

            int currentPages = 0;
            List<string> data = CouchbaseManager.Instance.NotificationRepository.RequestNotificationCache(1000);
            watch.Restart();

            if (data.Count > 0)
            {
                InternalLogger.WriteLog(String.Format("[Notif Worker]Load Notif :{0} ms , {1} notifs", watch.ElapsedMilliseconds, data.Count));

                List<MD.CloudConnect.CouchBaseProvider.Track> tracks = new List<Track>();
                List<string> cacheKey = new List<string>();
                List<MDData> result = null;
                foreach (string dNotif in data)
                {

                    try
                    {
                        result = MD.CloudConnect.Notification.Instance.Decode(dNotif);
                    }
                    catch
                    {
                        InternalLogger.WriteLog(String.Format("[Notif Worker] Decode Notif Error : {0}", dNotif.Substring(0, 50)));
                    }

                    try
                    {
                        foreach (MDData decodedData in result)
                        {
                            if (decodedData.Meta.Event == "track")
                            {
                                totalTrackGenerated += 1;
                                ITracking t = decodedData.Tracking;

                                //if (t.Asset == "351732054617629")
                                {
                                    MD.CloudConnect.CouchBaseProvider.Track newTrack = new Track()
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
                                            CouchbaseManager.Instance.TrackRepository.InsertListShouldBeTreat(tracks);
                                            tracks.Clear();
                                        }
                                    }
                                }
                            }
                        }
                        currentPages++;
                    }
                    catch (Exception ex)
                    {
                        InternalLogger.WriteLog(String.Format("[Notif Worker] Exception : {0}", ex.Message));
                    }
                }

                InternalLogger.WriteLog(String.Format("[Notif Worker] End Parse :{0} ms", watch.ElapsedMilliseconds));
                watch.Restart();
                if (tracks.Count > 0)
                    CouchbaseManager.Instance.TrackRepository.InsertListShouldBeTreat(tracks);
                CouchbaseManager.Instance.NotificationRepository.DropListRange(currentPages);
                InternalLogger.WriteLog(String.Format("[Notif Worker]Save Notif :{0} ms", watch.ElapsedMilliseconds));
                watch.Restart();
                cacheKey.Clear();
            }
            return totalTrackGenerated;
        }

        void _tracktimer_Elapsed(object sender, ElapsedEventArgs e)
        {
            if (_workInProgressForTrack)
            {
                return;
            }
            _workInProgressForTrack = true;
            try
            {
                Stopwatch watch = new Stopwatch();
                watch.Start();

                List<Track> tracks = RebuildHistory();
                if (tracks.Count > 0)
                {
                    InternalLogger.WriteLog(String.Format("[Track Worker] {0} tracks", tracks.Count));
                    InternalLogger.WriteLog(String.Format("[Track Worker] Rebuild :{0} ms", watch.ElapsedMilliseconds));
                }
                watch.Restart();
                if (tracks.Count > 0)
                {
                    SaveTrackUpdated(tracks);
                    InternalLogger.WriteLog(String.Format("[Track Worker] Save :{0} ms", watch.ElapsedMilliseconds));
                }
            }
            catch (Exception ex)
            {
                // InternalLogger.WriteLog(CouchbaseManager.Instance.DataListRepository.FlushLog());
                InternalLogger.WriteLog("[Track Worker] error" + ex.Message);
                InternalLogger.WriteLog("[Track Worker] stacktrace" + ex.ToString());
            }
            finally
            {
                _workInProgressForTrack = false;
            }
        }


        private void SaveTrackUpdated(List<Track> tracks)
        {
            List<Track> mustBeSaved = tracks.Where(x => x.Status != 0).ToList();
            InternalLogger.WriteLog(String.Format("[Track Worker]{0} tracks - waiting data", tracks.Count - mustBeSaved.Count));
            InternalLogger.WriteLog(String.Format("[Track Worker]{0} tracks - ok", mustBeSaved.Where(x => x.Status == 1).Count()));
            InternalLogger.WriteLog(String.Format("[Track Worker]{0} tracks - timeout", mustBeSaved.Where(x => x.Status == 2).Count()));
            InternalLogger.WriteLog(String.Format("[Track Worker]{0} tracks - bad time", mustBeSaved.Where(x => x.Status == 5).Count()));
            InternalLogger.WriteLog(String.Format("[Track Worker]{0} tracks - others", mustBeSaved.Where(x => x.Status != 2 && x.Status != 1 && x.Status != 5).Count()));
            if (mustBeSaved.Count > 0)
            {
                CouchbaseManager.Instance.TrackRepository.DropListRange(mustBeSaved, tracks);
                CouchbaseManager.Instance.TrackRepository.BulkUpsert(mustBeSaved);
            }
            // we are waiting the last moment to update cache
            //_cache.Clear();
            //_cache = tracks.Where(t => t.Status == 0).ToList();

            _deviceManager.FlushModification(_devicesRebuild);
            _devicesRebuild.Clear();
        }

        private List<Track> RebuildHistory()
        {
            List<Track> tracks = null;
            tracks = CouchbaseManager.Instance.TrackRepository.GetNotDecodedTrack(10000);

            if (tracks.Count > 0)
            {
                //if (_cache.Count() > 0)
                //    tracks.AddRange(_cache);

                Device device = null;
                var groupedTracks = tracks.GroupBy(x => x.Imei);

                foreach (var group in groupedTracks)
                {
                    device = _deviceManager.GetDevice(group.Key);
                    if (!_devicesRebuild.Contains(device.Imei))
                        _devicesRebuild.Add(device.Imei);
                    Track previous = null;
                    IEnumerable<Track> sortedTrack = group.OrderBy(x => x.ConnectionId).ThenBy(x => x.Index);
                    foreach (Track t in sortedTrack)
                    {
                        if (UpdateFieldHistory(device, t))
                            break;
                        previous = t;
                    }
                }
            }
            return tracks;
        }



        /// <summary>
        /// 0 : waiting more data
        /// 1 : Ok merge
        /// 2 : ok timeout
        /// 3 : same index
        /// 4 : data feed (special case)
        /// 5 : bad time
        /// </summary>
        /// <param name="d"></param>
        /// <param name="t"></param>
        /// <returns></returns>
        private int CanBeUpdate(Device d, Track t)
        {
            if (String.IsNullOrEmpty(t.ConnectionId))
                return 4;
            if (d.LastConnectionId == null)
                return 1;

            if (d.LastConnectionId == t.ConnectionId)
            {
                if (d.NextWaitingIndex == t.Index)
                    return 1;
                else if (d.LastIndex == t.Index)
                    return 3;
            }
            else
            {
                //hack connection id change but index still increase
                //if (d.NextWaitingIndex == t.Index || t.Index == 1)
                //    return 1;
                if (d.LastRecordedAt > t.Recorded_at && (d.LastRecordedAt.Ticks - t.Recorded_at.Ticks) > (TimeSpan.TicksPerMinute * 15))
                    return 5;
            }

            // timeout
            if ((DateTime.UtcNow.Ticks - t.Created_at.Ticks) > (TimeSpan.TicksPerMinute * 15))
            {
                //try to rebuild if recorded at is correct
                if (t.Recorded_at > d.LastRecordedAt)
                    return 1;
                else
                    return 2;
            }
            // wait more data
            return 0;
        }

        private void mergeField(Device d, Track t)
        {
            foreach (KeyValuePair<string, Field> item in t.Fields)
            {
                if (d.Fields.ContainsKey(item.Key))
                    d.Fields[item.Key] = item.Value;
                else d.Fields.Add(item.Key, item.Value);
            }

            foreach (KeyValuePair<string, Field> item in d.Fields)
            {
                if (_fieldManager.MustBeRebuild(item.Value.Key) && !t.Fields.ContainsKey(item.Key))
                {
                    t.Fields[item.Key] = item.Value;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="d"></param>
        /// <param name="t"></param>
        /// <param name="previous"></param>
        /// <returns> true => stop current process (waiting more data)</returns>
        private bool UpdateFieldHistory(Device d, Track t)
        {
            //already decoded but download in the view because all nodes are yet all updated
            if (t.Status > 0)
                return false;
            t.Updated_at = DateTime.UtcNow;
            int mergeStatus = CanBeUpdate(d, t);
           // if (mergeStatus != 1)
           //     InternalLogger.WriteLog(String.Format("[{0}] {5} / cloud : '{1}' connection id : '{2}' index : '{3}' mergeStatus : '{4}'", t.Recorded_at.ToString("dd HH:mm:ss"), t.CloudId, t.ConnectionId, t.Index, mergeStatus, d.Imei));

            if (mergeStatus == 4)
            {
                foreach (KeyValuePair<string, Field> item in t.Fields)
                {
                    if (d.Fields.ContainsKey(item.Key))
                        d.Fields[item.Key] = item.Value;
                    else d.Fields.Add(item.Key, item.Value);
                }
                t.Status = 4;
            }
            else
            {
                d.NextWaitingIndex = (t.Index.HasValue ? t.Index.Value : 0) + (uint)(t.Fields.Count + 1);
                t.NextWaitingIndex = d.NextWaitingIndex;
                if (t.Latitude != 0.0 && t.Longitude != 0.0)
                {
                    d.LastLatitude = t.Latitude;
                    d.LastLongitude = t.Longitude;
                }
                d.LastCloudId = t.CloudId;
                d.LastReceivedAt = t.Received_at;
                d.LastRecordedAt = t.Recorded_at;
                d.LastConnectionId = t.ConnectionId;
                d.LastIndex = t.Index.Value;
                d.UpdatedAt = DateTime.UtcNow;
                d.LastTrackId = CouchbaseManager.Instance.TrackRepository.BuildKey(t);

                if (mergeStatus == 1)
                {
                    mergeField(d, t);
                    t.Status = 1;
                }
                else if (mergeStatus == 0)
                {
                    return true;
                }
                else if (mergeStatus == 2)
                {
                    //specific case - timeout
                    // Add log here
                    t.Status = 2;
                }
                else if (mergeStatus == 3)
                {
                    // Add log here
                    //same connection id and index
                    t.Status = 3;
                }
                else if (mergeStatus == 5)
                {
                    // recorded at was in the past compare to the data already decoded for this device
                    t.Status = 5;
                }
            }
            return false;
        }
    }
}
