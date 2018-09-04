package com.gcs.tcclient;

import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.manager.DatasetManager;

public  class DataSetManagerFactory {
	private static volatile Dataset<String>  dataset = null;
	private static volatile Dataset<Long>  MASTER = null;
	private static volatile Dataset<String>  CROSSWALK = null;
	private static volatile Dataset<Long>  ADDRESS = null;
	private static volatile Dataset<Long>  SERVICE = null;
	private static volatile DatasetManager datasetManager2 = null;
	public static String CUST_MASTER_DS_NAME= "CUSTOMER_MASTER_V1";
	public static String CUST_CROSSWALK_DS_NAME= "CUSTOMER_CROSSWALK_V1";
	public static String ADDRESS_DS_NAME= "ADDRESS_V1";
	public static String SERVICE_DS_NAME= "SERVICE_V1";
	public static String tcURL=null;
	public static int timeout=30;
	
	//address master
	public static class CustMaster{
			public static final StringCellDefinition CUST_NAME = CellDefinition.defineString("CUST_NAME");
			public static final StringCellDefinition CUST_LNG_NAME = CellDefinition.defineString("CUST_LNG_NAME");
			public static final StringCellDefinition CUST_SHRT_NAME = CellDefinition.defineString("CUST_SHRT_NAME");
			public static final StringCellDefinition CUST_SURNM1 = CellDefinition.defineString("CUST_SURNM1");
			public static final StringCellDefinition CUST_SURNM2 = CellDefinition.defineString("CUST_SURNM2");
			public static final StringCellDefinition SURVIVORSHIP_DETAILS = CellDefinition.defineString("SURVIVORSHIP_DETAILS");
	}
	
	//cross walk cells
	
	public static class CustCrossWalk{
			public static final StringCellDefinition  NAME  = CellDefinition.defineString("NAME");
			public static final StringCellDefinition  ADDRESSLINE1 = CellDefinition.defineString("ADDRESSLINE1");
			public static final StringCellDefinition  ADDRESSLINE2 = CellDefinition.defineString("ADDRESSLINE2");
			public static final StringCellDefinition  ADMINISTRATIVEAREA = CellDefinition.defineString("ADMINISTRATIVEAREA");
			public static final StringCellDefinition  LOCALITY = CellDefinition.defineString("LOCALITY");
			public static final StringCellDefinition  POSTALCODE = CellDefinition.defineString("POSTALCODE");
			public static final StringCellDefinition  C_ADDRESSLINE1 = CellDefinition.defineString("C_ADDRESSLINE1");
			public static final StringCellDefinition  C_ADDRESSLINE2 = CellDefinition.defineString("C_ADDRESSLINE2");
			public static final StringCellDefinition  C_ADMINISTRATIVEAREA = CellDefinition.defineString("C_ADMINISTRATIVEAREA");
			public static final StringCellDefinition  C_LOCALITY = CellDefinition.defineString("C_LOCALITY");
			public static final StringCellDefinition  C_POSTALCODE = CellDefinition.defineString("C_POSTALCODE");
			public static final StringCellDefinition  C_COUNTRY = CellDefinition.defineString("C_COUNTRY");
			public static final StringCellDefinition  C_LATTITUDE = CellDefinition.defineString("C_LATTITUDE");
			public static final StringCellDefinition  C_LONGITUDE = CellDefinition.defineString("C_LONGITUDE");
			public static final StringCellDefinition  C_GEO_DISTANCE = CellDefinition.defineString("C_GEO_DISTANCE");
			public static final StringCellDefinition  MATCHING_RULE = CellDefinition.defineString("MATCHING_RULE");
			public static final StringCellDefinition  MATCHING_SCORE = CellDefinition.defineString("MATCHING_SCORE");
			public static final StringCellDefinition  ADDRESS_ID = CellDefinition.defineString("ADDRESS_ID");
			public static final StringCellDefinition  SOURCE_SYSTEM = CellDefinition.defineString("SOURCE_SYSTEM");
			public static final StringCellDefinition  SOURCE_SYSTEM_CUSTOMER_ID = CellDefinition.defineString("SOURCE_SYSTEM_CUSTOMER_ID");
			public static final StringCellDefinition  CONTRACT_ID = CellDefinition.defineString("CONTRACT_ID");
			public static final StringCellDefinition  SERVICE_ID = CellDefinition.defineString("SERVICE_ID");
			public static final StringCellDefinition  CUSTOMER_ID = CellDefinition.defineString("CUSTOMER_ID");
	}			
	//address cells
	public static class CustAddress{
			public static final StringCellDefinition  CUST_ADDR_ID = CellDefinition.defineString("CUST_ADDR_ID");
			public static final StringCellDefinition  ADDRESSLINE1 = CellDefinition.defineString("ADDRESSLINE1");
			public static final StringCellDefinition  ADDRESSLINE2 = CellDefinition.defineString("ADDRESSLINE2");
			public static final StringCellDefinition  ADMINISTRATIVEAREA = CellDefinition.defineString("ADMINISTRATIVEAREA");
			public static final StringCellDefinition  LOCALITY = CellDefinition.defineString("LOCALITY");
			public static final StringCellDefinition  POSTALCODE = CellDefinition.defineString("POSTALCODE");
			public static final StringCellDefinition  COUNTRY = CellDefinition.defineString("COUNTRY");
			public static final StringCellDefinition  CUST_ID = CellDefinition.defineString("CUST_ID");
			public static final StringCellDefinition  LONGITUDE = CellDefinition.defineString("LONGITUDE");
			public static final StringCellDefinition  LATTITUDE = CellDefinition.defineString("LATTITUDE");
			public static final StringCellDefinition  CUST_ADDR_EMAIL = CellDefinition.defineString("CUST_ADDR_EMAIL");
			public static final StringCellDefinition  CUST_ADDR_PHONE_NUMBER = CellDefinition.defineString("CUST_ADDR_PHONE_NUMBER");
			public static final StringCellDefinition  CUST_ADDR_FAX_NUMBER = CellDefinition.defineString("CUST_ADDR_FAX_NUMBER");
			public static final StringCellDefinition  WINKEY_ADDR = CellDefinition.defineString("WINKEY_ADDR");
			public static final StringCellDefinition  WINKEY_EMAIL = CellDefinition.defineString("WINKEY_EMAIL");
			public static final StringCellDefinition  WINKEY_PHNMBR = CellDefinition.defineString("WINKEY_PHNMBR");
			public static final StringCellDefinition  WINKEY_FXNMBR = CellDefinition.defineString("WINKEY_PHNMBR");
			public static final StringCellDefinition  CUST_ADDR_TYPE = CellDefinition.defineString("CUST_ADDR_TYPE");

	}  
	//service cells
	public static class CustService{
			//public static final StringCellDefinition  CONTRACT_ID  = CellDefinition.defineString("CONTRACT_ID");
			public static final StringCellDefinition  CUSTOMER_ID = CellDefinition.defineString("CUSTOMER_ID");
			public static final StringCellDefinition  SERVICE_ID = CellDefinition.defineString("SERVICE_ID");
			public static final StringCellDefinition  CUST_ADDR_ID = CellDefinition.defineString("CUST_ADDR_ID");
			public static final StringCellDefinition  CONTRACT_SOURCE_SYSTEM = CellDefinition.defineString("CONTRACT_SOURCE_SYSTEM");
			//public static final StringCellDefinition  SOURCE_SYSTEM_CONTRACT_ID = CellDefinition.defineString("SOURCE_SYSTEM_CONTRACT_ID");
			public static final StringCellDefinition  SERVICE_TYPE = CellDefinition.defineString("SERVICE_TYPE");
			public static final StringCellDefinition  SERVICE_TYPE_CODE = CellDefinition.defineString("SERVICE_TYPE_CODE");
			public static final StringCellDefinition  SERVICE_SOURCE_SYSTEM = CellDefinition.defineString("SERVICE_SOURCE_SYSTEM");
			public static final StringCellDefinition  SOURCE_SYSTEM_SERVICE_ID = CellDefinition.defineString("SOURCE_SYSTEM_SERVICE_ID");
			public static final StringCellDefinition  SERVICE_START_DATE = CellDefinition.defineString("SERVICE_START_DATE");
			public static final StringCellDefinition  SERVICE_END_DATE = CellDefinition.defineString("SERVICE_END_DATE");
			public static final StringCellDefinition  SERVICE_CODE = CellDefinition.defineString("SERVICE_CODE");
			public static final StringCellDefinition  SERVICE_DESCRIPTION = CellDefinition.defineString("SERVICE_DESCRIPTION");
			public static final StringCellDefinition  READY_RECKONER = CellDefinition.defineString("READY_RECKONER");
			public static final StringCellDefinition  INSTALLED_INDICATOR = CellDefinition.defineString("INSTALLED_INDICATOR");
			public static final StringCellDefinition  CONFIDENTIAL_INDICATOR = CellDefinition.defineString("CONFIDENTIAL_INDICATOR");
			public static final StringCellDefinition  FAMILY_SERVICE_CODE = CellDefinition.defineString("FAMILY_SERVICE_CODE");
			public static final StringCellDefinition  CATALOG_SERVICE_CODE = CellDefinition.defineString("CATALOG_SERVICE_CODE");
			public static final StringCellDefinition  CATALOG_INDICATOR = CellDefinition.defineString("CATALOG_INDICATOR");
			public static final StringCellDefinition  MODIFY_USER = CellDefinition.defineString("MODIFY_USER");
			public static final StringCellDefinition  MODIFY_DATE = CellDefinition.defineString("MODIFY_DATE");
	}
private static final Object ob1 = new java.lang.Object();
private static final Object ob2 = new java.lang.Object();
private static final Object ob3 = new java.lang.Object();
private static final Object ob4 = new java.lang.Object();
private static final Object ob5 = new java.lang.Object();

  
  public static void close(){
		if(dataset!=null){
			dataset.close();
			dataset=null;
		}
		if(MASTER!=null){
			MASTER.close();
			MASTER=null;
		}
		if(CROSSWALK!=null){
			CROSSWALK.close();
			CROSSWALK=null;
		}
		if(ADDRESS!=null){
			ADDRESS.close();
			ADDRESS=null;
		}
		if(SERVICE!=null){
			SERVICE.close();
			SERVICE=null;
		}
		if(datasetManager2!=null){
			datasetManager2.close();
			datasetManager2=null;
		}
		
		
	}
  public static void createCustCrossWalkDataSet() throws StoreException,Exception{
		//System.out.println
	  synchronized(ob1)
		{ if(CROSSWALK == null){
			DatasetConfiguration DS_CONFIG = getDatasetManager().datasetConfiguration() // <3>
			.offheap("second") // <4>
			.disk("customer")
			.index(CustCrossWalk.C_ADDRESSLINE1,  IndexSettings.BTREE)// <5>
			.index(CustCrossWalk.C_LOCALITY,IndexSettings.BTREE)
			.index(CustCrossWalk.SOURCE_SYSTEM_CUSTOMER_ID,IndexSettings.BTREE)
			.index(CustCrossWalk.SOURCE_SYSTEM,IndexSettings.BTREE)
			.index(CustCrossWalk.NAME,IndexSettings.BTREE)			
			.index(CustCrossWalk.CUSTOMER_ID,IndexSettings.BTREE)
			.build(); // <6>
			getDatasetManager().newDataset(CUST_CROSSWALK_DS_NAME,com.terracottatech.store.Type.STRING, DS_CONFIG);
			
				System.out.println("NEW: "+CUST_MASTER_DS_NAME+" created");
			}
		}
			

	
  }
  public static Dataset<String> getCustCrossWalkDataSet() throws Exception,URISyntaxException{
		if(CROSSWALK!= null){
			//System.out.println("REUSE: CUSTOMER_MASTER dataset Found in memory");
		}else {
			synchronized(ob1)
			{ if(CROSSWALK == null){
				CROSSWALK =	getDatasetManager().getDataset(CUST_CROSSWALK_DS_NAME, com.terracottatech.store.Type.STRING);
					System.out.println("NEW: Dataset not found in memory.Getting the dataset "+ CUST_CROSSWALK_DS_NAME);
				}
			}

		}	
		return CROSSWALK;
  }
  public static void createAddressDataSet() throws StoreException,Exception{
		//System.out.println
	  synchronized(ob2)
		{ if(ADDRESS == null){
			DatasetConfiguration DS_CONFIG = getDatasetManager().datasetConfiguration() // <3>
			.offheap("second") // <4>
			.disk("customer")
			.index(CustAddress.POSTALCODE,  IndexSettings.BTREE)// <5>
			.index(CustAddress.CUST_ID,IndexSettings.BTREE)
			.build(); // <6>
			getDatasetManager().newDataset(ADDRESS_DS_NAME,com.terracottatech.store.Type.LONG, DS_CONFIG);
			
				System.out.println("NEW: "+ADDRESS_DS_NAME+" created");
			}
		}
			

	
  }
  public static Dataset<Long> getAddressDataSet() throws Exception,URISyntaxException{
		if(ADDRESS!= null){
			//System.out.println("REUSE: CUSTOMER_MASTER dataset Found in memory");
		}else {
			synchronized(ob2)
			{ if(ADDRESS == null){
				ADDRESS =	getDatasetManager().getDataset(ADDRESS_DS_NAME, com.terracottatech.store.Type.LONG);
					System.out.println("NEW: Dataset not found in memory.Getting the dataset "+ ADDRESS_DS_NAME);
				}
			}

		}	
		return ADDRESS;
  }
  public static void createServiceDataSet() throws StoreException,Exception{
		//System.out.println
	  synchronized(ob3)
		{ if(SERVICE == null){
			DatasetConfiguration DS_CONFIG = getDatasetManager().datasetConfiguration() // <3>
			.offheap("second") // <4>
			.disk("customer")
			.index(CustService.CUST_ADDR_ID,  IndexSettings.BTREE)// <5>
			.index(CustService.CUSTOMER_ID,  IndexSettings.BTREE)
			.build(); // <6>
			getDatasetManager().newDataset(SERVICE_DS_NAME,com.terracottatech.store.Type.LONG, DS_CONFIG);
			
				System.out.println("NEW: "+SERVICE_DS_NAME+" created");
			}
		}
			

	
  }
  public static Dataset<Long> getServiceDataSet() throws Exception,URISyntaxException{
		if(SERVICE!= null){
			//System.out.println("REUSE: CUSTOMER_MASTER dataset Found in memory");
		}else {
			synchronized(ob3)
			{ if(SERVICE == null){
				SERVICE =	getDatasetManager().getDataset(SERVICE_DS_NAME, com.terracottatech.store.Type.LONG);
					System.out.println("NEW: Dataset not found in memory.Getting the dataset "+ SERVICE_DS_NAME);
				}
			}

		}	
		return SERVICE;
  }
  public static void createCustMasterDataSet() throws StoreException,Exception{
		//System.out.println
	  synchronized(ob4)
		{ if(MASTER == null){
			DatasetConfiguration CUST_MAST_CONFIG = getDatasetManager().datasetConfiguration() // <3>
			.offheap("second") // <4>
			.disk("customer")
			.index(CustMaster.CUST_NAME,  IndexSettings.BTREE)// <5>
			.build(); // <6>
			getDatasetManager().newDataset(CUST_MASTER_DS_NAME,com.terracottatech.store.Type.LONG, CUST_MAST_CONFIG);
			
				System.out.println("NEW: "+CUST_MASTER_DS_NAME+" created");
			}
		}
			

	
  }
  public static Dataset<Long> getCustMasterDataSet() throws Exception,URISyntaxException{
		if(MASTER!= null){
			//System.out.println("REUSE: CUSTOMER_MASTER dataset Found in memory");
		}else {
			synchronized(ob4)
			{ if(MASTER == null){
				MASTER =	getDatasetManager().getDataset(CUST_MASTER_DS_NAME, com.terracottatech.store.Type.LONG);
					System.out.println("NEW: Dataset not found in memory.Getting the dataset "+ CUST_MASTER_DS_NAME);
				}
			}

		}	
		return MASTER;
  }
  
  
  
		
	public static Dataset<String> getDataset() throws Exception,URISyntaxException{
		if(dataset!= null){
			//System.out.println("REUSE: Dataset Found in memory");
		}else {
			synchronized(ob5)
			{ if(dataset == null){
					dataset =	getDatasetManager().getDataset("customers", com.terracottatech.store.Type.STRING);
					System.out.println("NEW: Dataset not found in memory. Creating new dataset");
				}
			}

		}			
		
		return dataset;
	}
	public static void main(String[] args){
		tcURL= "terracotta://daehgcs28836.daedmz.loc:9410";
		try{//destoryAll();
			createAllDataSets();
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	public static void createAllDataSets()throws Exception{
		createAddressDataSet();
		createCustCrossWalkDataSet();
		createCustMasterDataSet();
		createServiceDataSet();
		
		
	}
	public static void destoryAll() throws Exception{
		
		getDatasetManager().destroyDataset("customers");
		//getDatasetManager().destroyDataset(CUST_CROSSWALK_DS_NAME);
		//getDatasetManager().destroyDataset(ADDRESS_DS_NAME);
		//getDatasetManager().destroyDataset(SERVICE_DS_NAME);

	}
	
	public static DatasetManager getDatasetManager() throws URISyntaxException,StoreException,Exception{
		if(datasetManager2!=null ){
			//System.out.println("REUSE: DatasetManager Found in memory");
		}else{
			
			if(tcURL == null || !tcURL.startsWith("terracotta"))  {
				tcURL = System.getProperty("watt.tcdb.customer.uri"); //try one more time. The property might be unloaed by classloader
				if(tcURL == null || !tcURL.startsWith("terracotta"))  
					throw new Exception("TCDB URL is not configured in extended settings watt.tcdb.customer.uri property");
			}
			java.net.URI clusterUri = new java.net.URI(tcURL);
			
			datasetManager2 = DatasetManager.clustered(clusterUri).withConnectionTimeout(timeout,TimeUnit.SECONDS).build() ;
			System.out.println("NEW: DatasetManager not Found in memory. Building the DSM");
		}
	    return datasetManager2;
				
	

}
}