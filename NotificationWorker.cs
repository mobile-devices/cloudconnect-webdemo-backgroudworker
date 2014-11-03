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
        private Timer _timer;

        public NotificationWorker()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            _timer = new Timer(10000);
            _timer.Elapsed += _timer_Elapsed;
            InternalLogger.WriteLog("Start Timer");
            _timer.Enabled = true;
        }

        void _timer_Elapsed(object sender, ElapsedEventArgs e)
        {
            InternalLogger.WriteLog("Log Event");
        }

        protected override void OnStop()
        {
            InternalLogger.WriteLog("Stop Timer");
            _timer.Enabled = false;
        }
    }
}
