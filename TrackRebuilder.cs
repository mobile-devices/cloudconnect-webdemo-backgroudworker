using CloudConnect.CouchBaseProvider;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace CloudConnect.BackgroundWorker
{
    partial class TrackRebuilder : ServiceBase
    {
        private bool _workInProgressForTrack = false;

        private Timer _tracktimer;

        private List<Track> _cache = new List<Track>();
        private string _lastTrackId = String.Empty;
        private string _nextTrackId = String.Empty;

        private FieldManager _fieldManager;
        private DeviceManager _deviceManager;

        private List<string> _devicesRebuild = new List<string>();

        private const int MAX_CYCLE = 15;
        private int _currentCycle = 1;

        public TrackRebuilder()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            InternalLogger.WriteLog("Track Rebuilder Worker v1.0");
            _fieldManager = FieldManager.Instance;
            _deviceManager = DeviceManager.Instance;

            _tracktimer = new Timer(2000);
            _tracktimer.Elapsed += _tracktimer_Elapsed;
            InternalLogger.WriteLog("Start Track Timer");
            _tracktimer.Enabled = true;
        }

        protected override void OnStop()
        {
            InternalLogger.WriteLog("Stop Track Timer");
            _tracktimer.Enabled = false;
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
                InternalLogger.WriteLog(String.Format("[Track Worker] {0} tracks", tracks.Count));
                InternalLogger.WriteLog(String.Format("[Track Worker] Rebuild :{0} ms / Last Doc ID : {1}", watch.ElapsedMilliseconds, _lastTrackId));
                watch.Restart();
                if (tracks.Count > 0)
                    SaveTrackUpdated(tracks);
                InternalLogger.WriteLog(String.Format("[Track Worker] Save :{0} ms", watch.ElapsedMilliseconds));

                _currentCycle++;
                if(_currentCycle >= MAX_CYCLE)
                {
                    _lastTrackId = String.Empty;
                    _cache.Clear();
                    _currentCycle = 1;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("[Track Worker] error" + ex.Message);
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
            InternalLogger.WriteLog(String.Format("[Track Worker]{0} tracks - others", mustBeSaved.Where(x => x.Status != 2 && x.Status != 1).Count()));
            if (mustBeSaved.Count > 0)
                CouchbaseManager.Instance.TrackRepository.BulkUpsert(mustBeSaved);

            // we are waiting the last moment to update cache and lastTrackId in case of the bulkinsert crash
            _cache.Clear();
            _cache = tracks.Where(t => t.Status == 0).ToList();
            _lastTrackId = _nextTrackId;

            _deviceManager.FlushModification(_devicesRebuild);
            _devicesRebuild.Clear();
        }

        private List<Track> RebuildHistory()
        {
            List<Track> tracks = CouchbaseManager.Instance.TrackRepository.GetNotDecodedTrack(1000, true, _lastTrackId);

            if (tracks.Count > 0)
            {
                if (!String.IsNullOrEmpty(_lastTrackId))
                {
                    _nextTrackId = tracks.Last().Id;
                    tracks.RemoveAt(0);
                    tracks.AddRange(_cache);
                }
                else
                    _nextTrackId = tracks.Last().Id;

                Device device = null;
                var groupedTracks = tracks.GroupBy(x => x.Imei);

                foreach (var group in groupedTracks)
                {
                    device = _deviceManager.GetDevice(group.Key);
                    if (!_devicesRebuild.Contains(device.Imei))
                        _devicesRebuild.Add(device.Imei);
                    Track previous = null;
                    IEnumerable<Track> sortedTrack = group.OrderBy(x => x.OrderingKey);
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
            if (d.LastRecordedAt > t.Recorded_at)
                return 5;
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
                if (d.NextWaitingIndex == t.Index || t.Index == 1)
                    return 1;
            }

            // timeout
            if ((DateTime.UtcNow.Ticks - t.Created_at.Ticks) > (TimeSpan.TicksPerMinute * 5))
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

            int mergeStatus = CanBeUpdate(d, t);
            if (mergeStatus == 1)
            {
                d.NextWaitingIndex = t.Index.Value + (uint)(t.Fields.Count + 1);
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

                Dictionary<string, Field> fieldNotRebuild = new Dictionary<string, Field>();
                foreach (KeyValuePair<string, Field> item in t.Fields)
                {
                    if (_fieldManager.MustBeRebuild(item.Value.Key))
                    {
                        if (d.Fields.ContainsKey(item.Key))
                            d.Fields[item.Key] = item.Value;
                        else d.Fields.Add(item.Key, item.Value);
                    }
                    else
                        fieldNotRebuild.Add(item.Key, item.Value);
                }
                t.Fields = d.Fields.Concat(fieldNotRebuild).ToDictionary(x => x.Key, x => x.Value);
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


                t.Status = 3;
            }
            else if (mergeStatus == 3)
            {
                // Add log here
                t.Status = 2;
            }
            else if (mergeStatus == 4)
            {
                // special case where we need to merge the fields with the previous track
                // Add log here
                t.Status = 4;
            }
            else if (mergeStatus == 5)
            {
                // recorded at was in the past compare to the data already decoded for this device
            }
            return false;
        }

    }
}
