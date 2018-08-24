package com.gcs.tcclient;

import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.manager.DatasetManager;

public  class DataSetManagerFactory {
	private static volatile Dataset<String>  dataset = null;
	private static volatile DatasetManager datasetManager = null;
	public static String tcURL=null;
	public static int timeout=300;
  private static final Object ob = new java.lang.Object();
	public static Dataset<String> getDataset() throws Exception,URISyntaxException{
		if(dataset!= null){
			//System.out.println("REUSE: Dataset Found in memory");
		}else {
			synchronized(ob)
			{ if(dataset == null){
					dataset =	getDatasetManager().getDataset("customers", com.terracottatech.store.Type.STRING);
					System.out.println("NEW: Dataset not found in memory. Creating new dataset");
				}
			}

		}			
		
		return dataset;
	}
	public static void close(){
		if(dataset!=null){
			dataset.close();
			dataset=null;
		}
		if(datasetManager!=null){
			datasetManager.close();
			datasetManager=null;
		}
		
		
	}
	
	private static DatasetManager getDatasetManager() throws URISyntaxException,StoreException,Exception{
		if(datasetManager!=null ){
			//System.out.println("REUSE: DatasetManager Found in memory");
		}else{
			
			if(tcURL == null || !tcURL.startsWith("terracotta"))  {
				tcURL = System.getProperty("watt.tcdb.customer.uri"); //try one more time. The property might be unloaed by classloader
				if(tcURL == null || !tcURL.startsWith("terracotta"))  
					throw new Exception("TCDB URL is not configured in extended settings watt.tcdb.customer.uri property");
			}
			java.net.URI clusterUri = new java.net.URI(tcURL);
			
			datasetManager = DatasetManager.clustered(clusterUri).withConnectionTimeout(timeout,TimeUnit.SECONDS).build() ;
			System.out.println("NEW: DatasetManager not Found in memory. Building the DSM");
		}
	    return datasetManager;
				
	

}
}
