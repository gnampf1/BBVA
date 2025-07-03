package de.dridders.bbva;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.rmi.RemoteException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Optional;

import javax.annotation.Resource;

import org.htmlunit.CookieManager;
import org.htmlunit.FailingHttpStatusCodeException;
import org.htmlunit.HttpMethod;
import org.htmlunit.Page;
import org.htmlunit.ProxyConfig;
import org.htmlunit.WebClient;
import org.htmlunit.WebRequest;
import org.htmlunit.WebResponse;
import org.htmlunit.html.HtmlPage;
import org.json.JSONArray;
import org.json.JSONObject;

import de.willuhn.datasource.GenericIterator;
import de.willuhn.datasource.rmi.DBIterator;
import de.willuhn.jameica.hbci.Settings;
import de.willuhn.jameica.hbci.SynchronizeOptions;
import de.willuhn.jameica.hbci.messaging.ImportMessage;
import de.willuhn.jameica.hbci.messaging.SaldoMessage;
import de.willuhn.jameica.hbci.rmi.Konto;
import de.willuhn.jameica.hbci.rmi.Umsatz;
import de.willuhn.jameica.hbci.synchronize.jobs.SynchronizeJob;
import de.willuhn.jameica.hbci.synchronize.jobs.SynchronizeJobKontoauszug;
import de.willuhn.jameica.security.Wallet;
import de.willuhn.jameica.system.Application;
import de.willuhn.logging.Logger;
import de.willuhn.util.ApplicationException;
import de.willuhn.util.I18N;
import de.willuhn.util.ProgressMonitor;

public class BBVASynchronizeJobKontoauszug extends SynchronizeJobKontoauszug implements BBVASynchronizeJob {
	private final static I18N i18n = Application.getPluginLoader().getPlugin(Plugin.class).getResources().getI18N();

	@Resource
	private BBVASynchronizeBackend backend = null;

	protected static Hashtable<String, String> passwortHashtable = new Hashtable<String, String>();

	/**
	   * @see org.jameica.hibiscus.sync.example.ExampleSynchronizeJob#execute()
	   */
	  @Override
	  public void execute() throws Exception
	  {
	    Konto konto = (Konto) this.getContext(CTX_ENTITY); // wurde von BBVASynchronizeJobProviderKontoauszug dort abgelegt
	    String myIban = konto.getIban();
	    
	    Logger.info("Rufe Ums\u00e4tze ab f\u00fcr " + backend.getName());
	  
	    ProgressMonitor monitor = backend.getCurrentSession().getProgressMonitor();
        SynchronizeOptions options = new SynchronizeOptions(konto);

        boolean forceSaldo  = false;
        Object forceSaldoObj = this.getContext(SynchronizeJobKontoauszug.CTX_FORCE_SALDO);
        if (forceSaldoObj != null) forceSaldo = (Boolean)forceSaldoObj;
        Boolean forceUmsatz = false;
        Object forceUmsatzObj = this.getContext(SynchronizeJobKontoauszug.CTX_FORCE_UMSATZ);
        if (forceUmsatzObj != null) forceUmsatz = (Boolean)forceUmsatzObj;
        
        Boolean fetchSaldo = options.getSyncSaldo() || forceSaldo;
        Boolean fetchUmsatz = options.getSyncKontoauszuege() || forceUmsatz;
        Logger.info("BBVA: Neue Synchronisierung wurde erkannt, mit folgenden Einstellungen: ");
        Logger.info("BBVA: forceSaldo: " + forceSaldo + ", forceUmsatz: " + forceUmsatz + ", fetchSaldo: " + fetchSaldo + ", fetchUmsatz: " + fetchUmsatz);
        
        options.setAutoSaldo(false);
        CookieManager cookieCache;
        DBIterator<Umsatz> umsaetze=null;

        monitor.setPercentComplete(0);
        monitor.log("Synchronisiere Konto: " + konto.getLongName());

        Logger.info("BBVA: Version 0.0.1 wurde gestartet ...");
        monitor.log("BBVA V0.0.1 wurde gestartet ...");
        monitor.log("******************************************************************************************************************\n");

        if (!fetchSaldo && !fetchUmsatz) {
            Logger.warn("BBVA: Neuer Sync wird nicht ausgef\u00fcrt da die Option 'Saldo aktualisieren' und 'Kontoausz\u00fcge (Ums\u00e4tze) abrufen' deaktiviert sind. Nichts zu tun");
            monitor.log("Neuer Sync wird nicht ausgef\u00fcrt da die Option 'Saldo aktualisieren' und 'Kontoausz\u00fcge (Ums\u00e4tze) abrufen' deaktiviert sind. Nichts zu tun");
        };
        
        Logger.debug("BBVA: Ums\u00e4tze von Hibiscus f\u00fcr Doppelbuchung-Checks holen ...");
        umsaetze = konto.getUmsaetze();
        Logger.debug("BBVA: Alle Buchungen aus dem Cache: "+umsaetze);
        monitor.setPercentComplete(1);
        
        
        WebClient webClient = new WebClient(new org.htmlunit.BrowserVersion.BrowserVersionBuilder(org.htmlunit.BrowserVersion.FIREFOX)
                .setAcceptLanguageHeader("de-DE")
                .setSecClientHintUserAgentHeader(null)
                .setSecClientHintUserAgentPlatformHeader(null)
                .setApplicationCodeName(null)
                .setCssAcceptHeader(null)
                .setHtmlAcceptHeader(null)
                .setImgAcceptHeader(null)
                .setScriptAcceptHeader(null)
                .setXmlHttpRequestAcceptHeader(null)
                .build());
        webClient.getOptions().setUseInsecureSSL(false);
        webClient.getOptions().setRedirectEnabled(true);
        webClient.getOptions().setJavaScriptEnabled(false);
        webClient.getOptions().setThrowExceptionOnScriptError(false);
        webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
        webClient.getOptions().setCssEnabled(false);

        Logger.debug("BBVA: es wird auf eine Proxy-Konfiguration gepr\u00fcft ...");
        Boolean useSystemProxy = Application.getConfig().getUseSystemProxy();
        String httpProxyHost = Application.getConfig().getProxyHost();
        Integer httpProxyPort = Application.getConfig().getProxyPort();
        String httpsProxyHost = Application.getConfig().getHttpsProxyHost();
        Integer httpsProxyPort = Application.getConfig().getHttpsProxyPort();
        ProxyConfig proxyConfig = null;

        Logger.info("BBVA: Proxy Einstellungen setzten ...");
        monitor.log("Proxy Einstellungen setzten ...");
        
        Logger.debug("BBVA: Jameica nutzt den System-Proxy: " + useSystemProxy);
        Logger.debug("BBVA: HTTP-Proxy Host von Jameica ist: " + httpProxyHost);
        Logger.debug("BBVA: HTTP-Proxy Port von Jameica ist: " + httpProxyPort);
        Logger.debug("BBVA: HTTPS-Proxy Host von Jameica ist: " + httpsProxyHost);
        Logger.debug("BBVA: HTTPS-Proxy Port von Jameica ist: " + httpsProxyPort);

        if (useSystemProxy == true) {
            java.lang.System.setProperty("java.net.useSystemProxies", "true");
            String SysProxyInfoHTTP = new java.lang.String(java.net.ProxySelector.getDefault().select(new java.net.URI("http://www.java.de")).get(0).toString());
            String SysProxyInfoHTTPS = new java.lang.String(java.net.ProxySelector.getDefault().select(new java.net.URI("https://www.mydrive.ch")).get(0).toString()); 
            int SysProxyFehler = 0;
            
            if ((SysProxyInfoHTTP.equals("DIRECT")) && (SysProxyInfoHTTPS.equals("DIRECT")))
            {
                monitor.log("Info-Warnung: Systemproxy-Einstellungen verwenden ist in Jameica eingestellt, es ist aber kein Proxy im System eingetragen!");
                Logger.info("BBVA: Systemproxy-Einstellungen verwenden ist in Jameica eingestellt, es ist aber kein Proxy im System eingetragen!");
                proxyConfig = new ProxyConfig();
            } 
            else 
            {
                String SysHttpsProxyHost = null;
                Integer SysHttpsProxyPort = null;
                String SysProxyHost = null;
                Integer SysProxyPort = null;
                
                if (!SysProxyInfoHTTP.equals("DIRECT")) 
                {
                    String[] SysProxyValuesString = SysProxyInfoHTTP.split(" @ "); //eg.: HTTP @ 10.96.3.105:8080
                    String SysProxyProtokol = SysProxyValuesString[0];
                    String SysProxySetting = SysProxyValuesString[1];
                    String[] SysProxyString = SysProxySetting.split(":"); //eg.: 10.96.3.105:8080
                    SysProxyHost = SysProxyString[0];
                    SysProxyPort = Integer.parseInt(SysProxyString[1]);
                }
                
                if (!SysProxyInfoHTTPS.equals("DIRECT")) 
                {
                    String[] SysHttpsProxyValuesString = SysProxyInfoHTTPS.split(" @ "); //eg.: HTTP @ 10.96.3.107:7070
                    String SysHttpsProxyProtokol = SysHttpsProxyValuesString[0];
                    String SysHttpsProxySetting = SysHttpsProxyValuesString[1];
                    String[] SysHttpsProxyString = SysHttpsProxySetting.split(":"); //eg.: 10.96.3.105:8080
                    SysHttpsProxyHost = SysHttpsProxyString[0];
                    SysHttpsProxyPort = Integer.parseInt(SysHttpsProxyString[1]);
                }
                
                if (SysHttpsProxyHost != null && SysHttpsProxyPort != -1)
                {
                	proxyConfig = new ProxyConfig(SysHttpsProxyHost, SysHttpsProxyPort, "https");
                    monitor.log("OK: Es wird der HTTPS-Proxy vom System benutzt");
                    Logger.info("BBVA: Es wird der HTTPS-Proxy vom System benutzt");
                } 
                else if (SysProxyHost != null && SysProxyPort != -1)
                {
                	proxyConfig = new ProxyConfig(SysProxyHost, SysProxyPort, "http");
                    monitor.log("Warnung: Es wird der HTTP-Proxy vom System benutzt. Sollte dieser kein HTTPS unterst\u00fczen gibt es Fehler!");
                    Logger.warn("BBVA: Es wird der HTTP-Proxy vom System benutzt. Sollte dieser kein HTTPS unterst\u00fczen gibt es Fehler!");
                }
                else 
                { 
                    throw new Exception("Systemproxy-Einstellungen verwenden ist gew\u00e4hlt: aber bei diesen fehlt offensichtlich ein Eintrag!");
                }
            }
        } 
        else if (httpsProxyHost != null && httpsProxyPort != -1)
        {
        	proxyConfig = new ProxyConfig(httpsProxyHost, httpsProxyPort, "https");
            monitor.log("OK: Es wird der HTTPS-Proxy von Jameica benutzt");
            Logger.info("BBVA: Es wird der HTTPS-Proxy von Jameica benutzt");
        } 
        else if (httpProxyHost != null && httpProxyPort != -1) 
        {
        	proxyConfig = new ProxyConfig(httpProxyHost, httpProxyPort, "http");
            monitor.log("Warnung: Es wird der HTTP-Proxy von Jameica benutzt. Sollte dieser kein HTTPS unterst\u00fczen gibt es Fehler!");
            Logger.warn("BBVA: Es wird der HTTP-Proxy von Jameica benutzt. Sollte dieser kein HTTPS unterst\u00fczen gibt es Fehler!");
        }
        
        // WebClient mit den den Proxy-Einstellungen anlegen
        if (proxyConfig != null)
        {
        	webClient.getOptions().setProxyConfig(proxyConfig);
        }
            
        Logger.info("BBVA: Verbindung vorbereitet");
        monitor.setPercentComplete(2);

        cookieCache = webClient.getCookieManager();
        if (!cookieCache.isCookiesEnabled()) 
        { 
        	cookieCache.setCookiesEnabled(true); 
        }

        String kundenNummer = konto.getKundennummer();

        Wallet wallet = de.willuhn.jameica.hbci.Settings.getWallet();
        Boolean cachePins = de.willuhn.jameica.hbci.Settings.getCachePin();
        Boolean storePins = de.willuhn.jameica.hbci.Settings.getStorePin();
        String walletAlias = "scripting.BBVA." + kundenNummer;
        
        monitor.log("Login f\u00fcr " + kundenNummer + " ...");
        
        String passwort = "";
        if (cachePins)
        { 
            passwort = passwortHashtable.get(kundenNummer); 
        } 
        else 
        {
            Logger.debug("Don't cache PINs");
            passwortHashtable.put(kundenNummer,null);
        }
        
        if (storePins) 
        {
            Logger.debug("Store PINs");
            passwort = (String)wallet.get(walletAlias); 
        } 
        else 
        {
            Logger.debug("Don't store PINs");
            if (wallet.get(walletAlias) != null) 
            { 
            	wallet.set(walletAlias,null); 
            }
        }
        
        try 
        {
            if (passwort == null || passwort.equals("")) 
            {
                Logger.info("BBVA: Passwort f\u00fcr Anmeldung "+kundenNummer+" wird abgefragt ...");			
                
                passwort = Application.getCallback().askPassword("Bitte geben Sie das Passwort f\u00fcr den BBVA-Zugang " + kundenNummer + " ein:");
            }
        } 
        catch(Exception err) 
        {
            Logger.error("BBVA: Login fehlgeschlagen! Passwort-Eingabe vom Benutzer abgebrochen");
            throw new  java.lang.Exception("Login fehlgeschlagen! Passwort-Eingabe vom Benutzer abgebrochen");
        }

    	Hashtable<String, String> headers = new Hashtable<String, String>();
        try 
        {
            WebResponse response = doRequest(webClient, "https://de-net.bbva.com/TechArchitecture/grantingTickets/V02", HttpMethod.POST, null, "application/json", "{\"authentication\":{\"consumerID\":\"00000366\",\"authenticationType\":\"121\",\"userID\":\"" + kundenNummer +"\",\"authenticationData\":[{\"authenticationData\":[\"" + passwort + "\"],\"idAuthenticationData\":\"password\"}]}}");
            JSONObject json = new JSONObject(response.getContentAsString());
            String authState = json.optString("authenticationState");
            if ("GO_ON".equals(authState))
            {
            	String multistepProcessId = json.optString("multistepProcessId");
            	response = doRequest(webClient, "https://de-net.bbva.com/TechArchitecture/grantingTickets/V02", HttpMethod.POST, null, "application/json", "{\"authentication\":{\"consumerID\":\"00000366\",\"authenticationType\":\"121\",\"userID\":\"" + kundenNummer +"\",\"multistepProcessId\":\""+ multistepProcessId + "\"}}");
            	json = new JSONObject(response.getContentAsString());
            	authState = json.optString("authenticationState");
                if ("GO_ON".equals(authState))
                {
                	multistepProcessId = json.optString("multistepProcessId");
                	
                	// OTP abfragen!
	                String otp = null;
	                do 
	                {
	                	otp = Application.getCallback().askUser("Bitte geben Sie den SMS-Code ein.", "SMS-Code");
	                	if (otp == null || otp.isEmpty()) 
	                	{
	                		throw new Exception("OTP-Eingabe wurde abgebrochen");
	                	}
	                }
	                while (otp == null || otp.length() != 6);
                	
                	headers.put("authenticationstate", multistepProcessId);
                	response = doRequest(webClient, "https://de-net.bbva.com/TechArchitecture/grantingTickets/V02", HttpMethod.POST, headers, "application/json", "{\"authentication\":{\"consumerID\":\"00000366\",\"authenticationType\":\"121\",\"userID\":\"" + kundenNummer +"\",\"multistepProcessId\":\"" + multistepProcessId + "\",\"authenticationData\":[{\"authenticationData\":[\"" + otp + "\"],\"idAuthenticationData\":\"otp\"}]}}");
                	json = new JSONObject(response.getContentAsString());
                	authState = json.optString("authenticationState");
	            }
            }
            
            String userId = null;
        	String personId = null;
            JSONObject userObj = json.optJSONObject("user");
            if (userObj != null)
            {
            	userId = userObj.optString("id");
            	JSONObject personObj = userObj.optJSONObject("person");
            	if (personObj != null)
            	{
            		personId = personObj.optString("id");
            	}
            }
            
            if (!"OK".equals(authState) && userId != null && personId != null)
            {
            	Logger.error("BBVA: Login fehlgeschlagen! AuthState ist " + authState + ", " + response.getContentAsString());
            	monitor.log("Login fehlgeschlagen, AuthState falsch (" + authState + ")!");
            	throw new  java.lang.Exception("Login fehlgeschlagen! AuthState falsch (" + authState + ")");
            }

            headers.clear();
        	headers.put("tsec", response.getResponseHeaderValue("tsec"));


            Hashtable<String, String> tsecheaders = new Hashtable<String, String>();
        	tsecheaders.put("x-tsec-token", response.getResponseHeaderValue("tsec"));
        	response = doRequest(webClient, "https://portunus-hub-es.live.global.platform.bbva.com/v1/tsec", HttpMethod.GET, tsecheaders, null, null);
        	JSONObject accessTokenJson = new JSONObject(response.getContentAsString());

        	if (response.getStatusCode() != 200 && accessTokenJson != null)
        	{
            	Logger.error("BBVA: Login fehlgeschlagen! " + response.getContentAsString());
            	monitor.log("Login fehlgeschlagen, TSEC-Abfrage gescheitert!");
            	throw new java.lang.Exception("Login fehlgeschlagen! TSEC-Abfrage gescheitert");
        	}
        	
            if (cachePins) { passwortHashtable.put(kundenNummer, passwort); }
            if (storePins) { wallet.set(walletAlias, passwort); }

            monitor.setPercentComplete(5); 

            Logger.info("BBVA: Login f\u00fcr " + kundenNummer + " war erfolgreich");
            monitor.log("Login war erfolgreich");
            response = doRequest(webClient, "https://de-net.bbva.com/financial-overview/v1/financial-overview?customer.id=" + personId + "&showSicav=false&showPending=true", HttpMethod.GET, headers, null, null);
            json = new JSONObject(response.getContentAsString()).optJSONObject("data");
            JSONArray contracts = json.getJSONArray("contracts");
            var ktoContract = new Object() { JSONObject value = null; };
            contracts.forEach(c -> 
            {
            	JSONObject contract = (JSONObject)c;
            	contract.optJSONArray("formats").forEach(f -> 
            	{
            		JSONObject format = (JSONObject)f;
					if ("IBAN".equals(format.optJSONObject("numberType").optString("id")) && myIban.equals(format.optString("number")))
					{
						ktoContract.value = contract;
					}
            	});
            });

            if (ktoContract.value == null)
            {
            	Logger.error("BBVA: Konto mit IBAN " + konto.getIban() + " nicht gefunden!" + response.getContentAsString());
            	monitor.log("Konto mit IBAN " + konto.getIban() + " nicht gefunden!");
            	throw new java.lang.Exception("Konto mit IBAN " + konto.getIban() + " nicht gefunden!");
            }

            String contractId = ktoContract.value.optString("id");
            ktoContract.value.optJSONObject("detail").optJSONArray("specificAmounts").forEach(sa ->
            {
            	JSONObject specificAmounts = (JSONObject)sa;
            	double saldo = specificAmounts.getJSONArray("amounts").getJSONObject(0).optDouble("amount");
            	try 
            	{
	            	if ("availableBalance".equals(specificAmounts.optString("id")))
	            	{
	            		konto.setSaldoAvailable(saldo);
	            	}
	            	else if ("currentBalance".equals(specificAmounts.optString("id")))
	            	{
	            		konto.setSaldo(saldo);
	            	}
            	}
            	catch (RemoteException ex) 
            	{            	
    	        	Logger.error("BBVA: Fehler beim Setzen vom Saldo: " + ex.toString());
    	        	monitor.log("Fehler beim Setzen vom Saldo");
            	}
            });
            
            response = doRequest(webClient, "https://de-net.bbva.com/accounts/v0/accounts/" + contractId + "/dispokredits/validations/", HttpMethod.GET, headers, null, null);
            JSONObject dispo = new JSONObject(response.getContentAsString()).optJSONObject("data");
            if (dispo != null)
            {
            	dispo.optJSONArray("dispokreditAmounts").forEach(d ->
            	{
            		JSONObject dispoAmount = (JSONObject)d;
            		if ("stdAuthDispoAmount".equals(dispoAmount.getString("id")))
            		{
            			try 
            			{
            				konto.setSaldoAvailable(konto.getSaldoAvailable() + dispoAmount.getJSONObject("amount").getDouble("amount"));
            			} catch (RemoteException ex)
            			{
            	        	Logger.error("BBVA: Fehler beim Setzen vom Dispo-Saldo: " + ex.toString());
            	        	monitor.log("Fehler beim Setzen vom Dispo-Saldo");
            			}
            		}
            	});
            }
            
            konto.store();
            Application.getMessagingFactory().sendMessage(new SaldoMessage(konto));

            int page = 0;
            int numPages = 0;
        	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            ArrayList<Umsatz> neueUmsaetze = new ArrayList<Umsatz>();
            do 
            {
            	response = doRequest(webClient, "https://de-net.bbva.com/accountTransactions/V02/accountTransactionsAdvancedSearch?pageSize=40&paginationKey=" + page, HttpMethod.POST, headers, "application/json", "{\"accountContracts\":[{\"contract\":{\"id\":\"" + contractId + "\"}}],\"customer\":{\"id\":\"" + personId + "\"},\"orderField\":\"DATE_FIELD\",\"orderType\":\"DESC_ORDER\",\"searchType\":\"SEARCH\"}");
            	json = new JSONObject(response.getContentAsString());
            	numPages = json.optJSONObject("pagination").optInt("numPages");
            	
            	json.optJSONArray("accountTransactions").forEach(t -> 
            	{
            		JSONObject transaction = (JSONObject)t;
            		
            		try 
            		{
	                	Umsatz newUmsatz = (Umsatz) Settings.getDBService().createObject(Umsatz.class,null);
	                	newUmsatz.setKonto(konto);
	                	newUmsatz.setArt(transaction.optJSONObject("concept").optString("name"));
	                	newUmsatz.setBetrag(transaction.optJSONObject("amount").optDouble("amount"));
	                	newUmsatz.setDatum(dateFormat.parse(transaction.optString("transactionDate")));
	                	newUmsatz.setSaldo(transaction.optJSONObject("balance").optJSONObject("accountingBalance").optDouble("amount"));
	                	newUmsatz.setTransactionId(transaction.optString("id"));
	                	newUmsatz.setValuta(dateFormat.parse(transaction.optString("valueDate")));
	                	newUmsatz.setZweck(transaction.optString("humanConceptName"));
	                	newUmsatz.setZweck2(transaction.optString("humanExtendedConceptName"));
	            			
	            		String detailSourceKey = transaction.optJSONObject("origin").optString("detailSourceKey");
	            		String detailSourceId = transaction.optJSONObject("origin").optString("detailSourceId");
	            		if (detailSourceKey != null && !"".equals(detailSourceKey) && !detailSourceKey.contains(" ") && !"KPSA".equals(detailSourceId))
	            		{
	                    	WebResponse detailResponse = doRequest(webClient, "https://de-net.bbva.com/transfers/v0/transfers/" + detailSourceKey + "-RE-" + contractId + "/", HttpMethod.GET, headers, null, null);
	                    	if (detailResponse != null)
	                    	{
		                    	JSONObject details = new JSONObject(detailResponse.getContentAsString()).optJSONObject("data");
		
		                    	JSONObject gegenkto = details.optJSONObject("sender");
		                    	JSONObject eigenkto = details.optJSONObject("receiver");
		                    	if ("BBVADEFFXXX".equals(gegenkto.optJSONObject("bank").optString("BICCode")))
		                    	{
		                    		eigenkto = gegenkto;
		                    		gegenkto = details.optJSONObject("receiver");
		                    	}
		                    	
		                    	newUmsatz.setCustomerRef(eigenkto.optString("reference"));
		                    	newUmsatz.setGegenkontoBLZ(gegenkto.optJSONObject("bank").optString("BICCode"));
		                    	String name = gegenkto.optString("fullName"); 
		                    	if (name != null && !"".equals(name))
		                    	{
		                    		newUmsatz.setGegenkontoName(name);
		                    	}
		                    	else
		                    	{
		                    		newUmsatz.setGegenkontoName(gegenkto.optString("alias"));
		                    	}
		                    	newUmsatz.setGegenkontoNummer(gegenkto.optJSONObject("contract").optString("number"));
	                    	}
	            		}
            		
	            		neueUmsaetze.add(newUmsatz);
            		}
            		catch (Exception ex)
            		{
        	        	Logger.error("BBVA: Fehler beim Anlegen vom Umsatz: " + ex.toString());
        	        	monitor.log("Fehler beim Anlegen vom Umsatz");
            		}
            	});
            	
            	page++;
            } while (page < numPages);
            
            monitor.setPercentComplete(75); 
            Logger.info("BBVA: Kontoauszug erfolgreich. Importiere Daten ...");
            monitor.log("Kontoauszug erfolgreich. Importiere Daten ...");

            for (int i = neueUmsaetze.size() - 1; i >= 0; i--)
            {
            	Umsatz umsatz = neueUmsaetze.get(i); 
            	if (!isDuplikat(umsaetze, umsatz)) 
            	{
		    		umsatz.store();
		    		Application.getMessagingFactory().sendMessage(new ImportMessage(umsatz));
            	}
            }
        } 
        catch (Exception ex) 
        {
        	Logger.error("BBVA: Fehler beim Abrufen der Ums\u00e4tze: " + ex.toString());
        	monitor.log("Fehler: "+ex.toString());
        }
        finally
        {
        	// Logout
        	doRequest(webClient, "https://de-net.bbva.com/TechArchitecture/grantingTickets/V02", HttpMethod.DELETE, headers, null, null);
        }
        monitor.log("******************************************************************************************************************\n\n\n");
        monitor.addPercentComplete(100); 
	  }

	private WebResponse doRequest(WebClient webClient, String url, HttpMethod method, Hashtable<String, String> headers,
			String contentType, String data) throws URISyntaxException, FailingHttpStatusCodeException, IOException {
		WebRequest request = new WebRequest(new java.net.URI(url).toURL(), method);
		if (headers != null) {
			request.setAdditionalHeaders(headers);
		} else {
			request.setAdditionalHeaders(new Hashtable<String, String>());
		}
		request.setAdditionalHeader("Accept", "application/json");
		request.setAdditionalHeader("Accept-Language", "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7");
		if (contentType != null && data != null) {
			request.setAdditionalHeader("content-type", contentType);
			request.setRequestBody(data);
		}
		Page page = webClient.getPage(request);
		if (page == null) {
			Logger.error("BBVA: Abruf gescheitert " + request.getUrl());
			return null;
		} else if (page.getWebResponse().getStatusCode() != 200) {
			Logger.error("BBVA: Abruf gescheitert " + request.getUrl() + " / " + page.getWebResponse().getStatusCode()
					+ " / " + page.getWebResponse().getContentAsString());
			return null;
		}

		return page.getWebResponse();
	}

	protected boolean isDuplikat(DBIterator<Umsatz> umsaetze, Umsatz buchung) throws RemoteException 
	{
		umsaetze.begin();
		while (umsaetze.hasNext()) 
		{
			Umsatz buchung2 = umsaetze.next();

			if (buchung2.getTransactionId().equals(buchung.getTransactionId()))
			{
				return true;
			}
		}
		return false;
	}
}
