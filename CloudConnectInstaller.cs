using System.ComponentModel;
using System.Configuration.Install;
using System.ServiceProcess;

namespace CloudConnect.BackgroundWorker
{
    [RunInstaller(true)]
    public class CloudConnectInstaller : Installer
    {
        private ServiceProcessInstaller processInstaller;
        private ServiceInstaller serviceInstaller;

        public CloudConnectInstaller()
        {
            processInstaller = new ServiceProcessInstaller();
            serviceInstaller = new ServiceInstaller();

            processInstaller.Account = ServiceAccount.LocalSystem;
            serviceInstaller.StartType = ServiceStartMode.Manual;
            serviceInstaller.ServiceName = "CloudConnect.NotificationWorker"; //must match CronService.ServiceName

            Installers.Add(serviceInstaller);
            Installers.Add(processInstaller);
        }
    }
}