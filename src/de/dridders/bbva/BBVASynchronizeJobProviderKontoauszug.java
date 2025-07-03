package de.dridders.bbva;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.Resource;

import de.willuhn.annotation.Lifecycle;
import de.willuhn.annotation.Lifecycle.Type;
import de.willuhn.jameica.hbci.SynchronizeOptions;
import de.willuhn.jameica.hbci.rmi.Konto;
import de.willuhn.jameica.hbci.synchronize.jobs.SynchronizeJob;
import de.willuhn.jameica.hbci.synchronize.jobs.SynchronizeJobKontoauszug;
import de.willuhn.logging.Logger;

public class BBVASynchronizeJobProviderKontoauszug implements BBVASynchronizeJobProvider
{
  @Resource
  private BBVASynchronizeBackend backend = null;

  private final static List<Class<? extends SynchronizeJob>> JOBS = new ArrayList<Class<? extends SynchronizeJob>>()
  {{
    add(BBVASynchronizeJobKontoauszug.class);
  }};

  /**
   * @see de.willuhn.jameica.hbci.synchronize.SynchronizeJobProvider#getSynchronizeJobs(de.willuhn.jameica.hbci.rmi.Konto)
   */
  @Override
  public List<SynchronizeJob> getSynchronizeJobs(Konto k)
  {
    Class<SynchronizeJobKontoauszug> type = SynchronizeJobKontoauszug.class;
    
    List<SynchronizeJob> jobs = new LinkedList<SynchronizeJob>();
    for (Konto kt:backend.getSynchronizeKonten(k))
    {
      try
      {
        if (!backend.supports(type,k)) // Checken, ob das ein passendes Konto ist
          continue;

        final SynchronizeOptions options = new SynchronizeOptions(kt);

        if (!options.getSyncKontoauszuege()) // Sync-Option zum Kontoauszugs-Abruf aktiv?
          continue;
        
        SynchronizeJobKontoauszug job = (SynchronizeJobKontoauszug) backend.create(type,kt); // erzeugt eine Instanz von ExampleSynchronizeJobKontoauszug
        job.setContext(SynchronizeJob.CTX_ENTITY,kt);
        jobs.add(job);
      }
      catch (Exception e)
      {
        Logger.error("unable to load synchronize jobs",e);
      }
    }

    return jobs;
  }

  @Override
  public List<Class<? extends SynchronizeJob>> getJobTypes()
  {
    return JOBS;
  }

	@Override
	public boolean supports(Class<? extends SynchronizeJob> type, Konto k) {
		try {
			return k.getBLZ().equals("50031900");
		} catch (RemoteException e) {
			return false;
		}
	}

  @Override
  public int compareTo(Object o)
  {
    return 1;
  }
}