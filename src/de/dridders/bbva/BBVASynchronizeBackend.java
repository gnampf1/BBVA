package de.dridders.bbva;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.rmi.RemoteException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.htmlunit.CookieManager;
import org.htmlunit.HttpMethod;
import org.htmlunit.Page;
import org.htmlunit.ProxyConfig;
import org.htmlunit.WebClient;
import org.htmlunit.WebRequest;
import org.htmlunit.html.HtmlPage;
import org.json.JSONArray;
import org.json.JSONObject;

import de.willuhn.annotation.Lifecycle;
import de.willuhn.annotation.Lifecycle.Type;
import de.willuhn.datasource.rmi.DBIterator;
import de.willuhn.jameica.hbci.Settings;
import de.willuhn.jameica.hbci.SynchronizeOptions;
import de.willuhn.jameica.hbci.messaging.ImportMessage;
import de.willuhn.jameica.hbci.messaging.SaldoMessage;
import de.willuhn.jameica.hbci.rmi.Konto;
import de.willuhn.jameica.hbci.rmi.Umsatz;
import de.willuhn.jameica.hbci.synchronize.AbstractSynchronizeBackend;
import de.willuhn.jameica.hbci.synchronize.SynchronizeBackend;
import de.willuhn.jameica.hbci.synchronize.SynchronizeEngine;
import de.willuhn.jameica.hbci.synchronize.SynchronizeJobProvider;
import de.willuhn.jameica.hbci.synchronize.jobs.SynchronizeJob;
import de.willuhn.jameica.hbci.synchronize.jobs.SynchronizeJobKontoauszug;
import de.willuhn.jameica.security.Wallet;
import de.willuhn.jameica.system.Application;
import de.willuhn.logging.Logger;
import de.willuhn.util.ApplicationException;
import de.willuhn.util.ProgressMonitor;

@Lifecycle(Type.CONTEXT)
public class BBVASynchronizeBackend extends AbstractSynchronizeBackend{
    @Resource
    private SynchronizeEngine engine = null;

    @Override
    protected JobGroup createJobGroup(Konto k) {
        return new BBVAJobGroup(k);
    }
    
    @Override
    protected Class<? extends SynchronizeJobProvider> getJobProviderInterface() {
        return BBVASynchronizeJobProvider.class;
    }
    
    /**
     * @see de.willuhn.jameica.hbci.synchronize.AbstractSynchronizeBackend#getPropertyNames(de.willuhn.jameica.hbci.rmi.Konto)
     */
    @Override
    public List<String> getPropertyNames(Konto konto)
    {
    	return null;
    }

    @Override
    public List<Konto> getSynchronizeKonten(Konto k)
    {
        List<Konto> list = super.getSynchronizeKonten(k);
        List<Konto> result = new ArrayList<Konto>();
        
        // Wir wollen nur die Offline-Konten und jene, bei denen Scripting explizit konfiguriert ist
        for (Konto konto:list)
        {
            if (konto != null)
            {
            	SynchronizeBackend backend = engine.getBackend(konto);
            	if (backend != null && backend.equals(this))
            	{
	                result.add(konto);
            	}
            }
        }
        
        return result;
    }

    @Override
    public String getName()
    {
        return "BBVA";
    }

    protected class BBVAJobGroup extends JobGroup
    {
        protected BBVAJobGroup(Konto k) {
            super(k);
        }
        
        @Override
        protected void sync() throws Exception
        {
			////////////////////////////////////////////////////////////////////
			// lokale Variablen
			ProgressMonitor monitor = worker.getMonitor();
			String kn               = this.getKonto().getLongName();
			
			int step = 100 / worker.getSynchronization().size();
			////////////////////////////////////////////////////////////////////
			
			try
			{
				this.checkInterrupted();
				
				monitor.log(" ");
				monitor.log(i18n.tr("Synchronisiere Konto: {0}",kn));
				
				Logger.info("processing jobs");
				for (Object job:this.jobs)
				{
					this.checkInterrupted();
					
					BBVASynchronizeJob j = (BBVASynchronizeJob) job;
					j.execute();
					
					monitor.addPercentComplete(step);
				}
			}
			catch (Exception e)
			{
				throw e;
			}
        }
    }
}
