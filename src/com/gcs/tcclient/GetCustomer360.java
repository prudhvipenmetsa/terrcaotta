package com.gcs.tcclient;

import java.util.Optional;

import com.gcs.tcclient.DataSetManagerFactory.CustAddress;
import com.gcs.tcclient.DataSetManagerFactory.CustCrossWalk;
import com.gcs.tcclient.DataSetManagerFactory.CustMaster;
import com.gcs.tcclient.DataSetManagerFactory.CustService;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.function.BuildablePredicate;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.stream.MutableRecordStream;
import com.wm.data.*;

public class GetCustomer360 {
	public static final void getCustomer3602(IData pipeline) throws Exception {
		IDataCursor pipelineCursor = pipeline.getCursor();

		/*
		Optional<String> CUST_NAME = Optional.ofNullable(null);//IDataUtil.getString( pipelineCursor, "CUST_NAME" ));
		Optional<String> CUST_ID =  Optional.ofNullable(""+17477911); //66032589
		Optional<String> ADDRESS_NAME = Optional.ofNullable(null);
		Optional<String> CITY = Optional.ofNullable(null);
		 */
		Optional<String> CUST_NAME = Optional.ofNullable(IDataUtil.getString( pipelineCursor, "CUST_NAME" ));
		Optional<String> CUST_ID =  Optional.ofNullable(IDataUtil.getString( pipelineCursor, "CUST_ID" ));
		Optional<String> ADDRESS_NAME =  Optional.ofNullable(IDataUtil.getString( pipelineCursor, "ADDRESS_NAME" ));
		Optional<String> SRC_SYSTEM =  Optional.ofNullable(IDataUtil.getString( pipelineCursor, "SRC_SYSTEM" ));
		Optional<String> CITY =  Optional.ofNullable(IDataUtil.getString( pipelineCursor, "CITY" ));
		Optional<String> singleAddressOnly =  Optional.ofNullable(IDataUtil.getString( pipelineCursor, "singleAddressOnly" ));

		java.util.List<Record<String>> multiRecs = null ;
		Optional<Record<String>> singleRec = null ;
		boolean isSingleRec =false;
		//long start_time = System.nanoTime();
		long start_time = System.currentTimeMillis();
		try{
			Dataset<String> crossWalkDS = com.gcs.tcclient.DataSetManagerFactory.getCustCrossWalkDataSet();
			DatasetWriterReader<String> crosswalkReader = crossWalkDS.writerReader();
			try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){				// add Filters
				BuildablePredicate<Record<?>>  filterPredicate = addFilterPredicate(null,CustCrossWalk.SOURCE_SYSTEM_CUSTOMER_ID,CUST_ID);
				filterPredicate = addFilterPredicate(filterPredicate,CustCrossWalk.SOURCE_SYSTEM,SRC_SYSTEM);
				filterPredicate = addFilterPredicate(filterPredicate,CustCrossWalk.NAME,CUST_NAME);
				filterPredicate = addFilterPredicate(filterPredicate,CustCrossWalk.C_ADDRESSLINE1,ADDRESS_NAME);
				filterPredicate = addFilterPredicate(filterPredicate,CustCrossWalk.C_LOCALITY,CITY);
				if(filterPredicate==null){
					IDataUtil.put( pipelineCursor, "status", createStatusRecord("ERROR","No Filter conditions found in the input. Please provide at least one filter condition") );
				}else{
					if(CUST_ID.isPresent()) {//get only one record
						singleRec = 
								recordStream.explain(System.out::println)
								.filter(filterPredicate).findAny();
						isSingleRec=true;
					}else{
						multiRecs = 						
								recordStream.batch(50)
								.explain(System.out::println)
								.filter(filterPredicate)
								.collect(java.util.stream.Collectors.toList());	

					}		
					java.util.HashSet<String> custIdList = new java.util.HashSet<String>();
					if(isSingleRec&&singleRec.isPresent()){
						custIdList.add(singleRec.get().get(CustCrossWalk.CUSTOMER_ID).get());
					}else{
						if(multiRecs!=null)
							for(Record<String> record: multiRecs)
								custIdList.add(record.get(CustCrossWalk.CUSTOMER_ID).get());
					}

					//Now get all the customer for the given custId list				

					// customers.customer
					java.util.ArrayList<IData>	customerAL = new java.util.ArrayList<IData>();
					if(custIdList.size()==0){
						IDataUtil.put( pipelineCursor, "status", createStatusRecord("ERROR","No customer records found for the given search criteria") );
					}else{
						IData	customers = IDataFactory.create();
						IDataCursor customersCursor = customers.getCursor();
						Dataset<Long> custDS = com.gcs.tcclient.DataSetManagerFactory.getCustMasterDataSet();
						for(String custID: custIdList){		
							DatasetReader<Long> custReader = custDS.reader();	
							IData customer = IDataFactory.create();
							IDataCursor customerCursor = customer.getCursor();							
							IDataUtil.put(customerCursor,"master",getCustomerIDataByKey(custReader,custID));
							//now get the Addresses
							java.util.Hashtable<Long,IData>  addressAL = getAddressIDataByCustomerId(null,singleAddressOnly,ADDRESS_NAME,custID) ;
							if(addressAL.size()>0)//insert Addresses
								IDataUtil.put(customerCursor,"address",addressAL.values().toArray(new IData[addressAL.size()]));
							customerCursor.destroy();
							customerAL.add(customer);
						}
						if(customerAL.size()>0){
							IDataUtil.put(customersCursor,"customer",customerAL.toArray(new IData[customerAL.size()]));
							IDataUtil.put( pipelineCursor, "customers", customers );
							IDataUtil.put( pipelineCursor, "status", createStatusRecord("OK","Successfully retrieved "+customerAL.size()+" customers.") );
						}else{
							IDataUtil.put( pipelineCursor, "status", createStatusRecord("ERROR","No customer records found for the given search criteria") );
						}
						customersCursor.destroy();
					}

					long end_time = System.currentTimeMillis();
				}
			}




		}catch(Exception e){
			e.printStackTrace(); //TBD. Should be deleted beforee golive
			IDataUtil.put( pipelineCursor, "status", createStatusRecord("ERROR",e.getMessage()));

		}


		pipelineCursor.destroy();





	}
	public static void main(String[] args){
		//searchCrosswalk();
		DataSetManagerFactory.tcURL= "terracotta://daehgcs28835.daedmz.loc:9410";
		// get360("sourceSyste","custID", "name", "addess", "city", "isAddressOnly");
		//name only
		try{
			printAllCrossWalk(null,"52207", null, null, null, "false");//ANDALUZA SUPERMERCAD
			//get360(null,"52207", null, null, null, "false");//"MARE NOSTRUM RESORT"
			// getAddressIDataByCustomerId(Optional.ofNullable(null) ,Optional.ofNullable(null),"107") ;
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	public static void getCustomer360ByName(){

	}


	public static void searchCrosswalk(){
		IData idata = IDataFactory.create();
		IDataCursor pipelineCursor = idata.getCursor();
		Optional<String> CUST_NAME = Optional.ofNullable(null);//IDataUtil.getString( pipelineCursor, "CUST_NAME" ));
		Optional<String> CUST_ID =  Optional.ofNullable(""+17477911); //66032589
		Optional<String> ADDRESS_NAME = Optional.ofNullable(null);
		Optional<String> CITY = Optional.ofNullable(null);
		Optional<String> SRC_SYSTEM = Optional.ofNullable(null);
		java.util.List<Record<String>> multiRecs = null ;
		Optional<Record<String>> singleRec = null ;
		boolean isSingleRec =false;
		//long start_time = System.nanoTime();
		long start_time = System.currentTimeMillis();
		try(Dataset<String> crossWalkDS = com.gcs.tcclient.DataSetManagerFactory.getCustCrossWalkDataSet();){

			DatasetWriterReader<String> crosswalkReader = crossWalkDS.writerReader();
			if(CUST_ID.isPresent() && SRC_SYSTEM.isPresent()){ // get the value using key

				try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){
					singleRec = 
							crosswalkReader.get(SRC_SYSTEM.get()+"_"+CUST_ID.get());


					isSingleRec=true;
				}

			}else if(CUST_NAME.isPresent() && CUST_ID.isPresent() && ADDRESS_NAME.isPresent() &&CITY.isPresent()){
				try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){
					singleRec = 

							recordStream.filter(CustCrossWalk.NAME.value().is(CUST_NAME.get()))	
							.filter(CustCrossWalk.SOURCE_SYSTEM_CUSTOMER_ID.value().is(CUST_ID.get()))	
							.filter(CustCrossWalk.C_ADDRESSLINE1.value().is(ADDRESS_NAME.get()))
							.filter(CustCrossWalk.C_LOCALITY.value().is(CITY.get().toUpperCase()))
							//.collect(java.util.stream.Collectors.toList());	
							.findAny(); //find any one record to get the cust master id
					isSingleRec=true;		
				}
				// all four
			}else if(CUST_NAME.isPresent() && CUST_ID.isPresent() && ADDRESS_NAME.isPresent() ){
				try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){
					singleRec =

							recordStream.filter(CustCrossWalk.NAME.value().is(CUST_NAME.get()))	
							.filter(CustCrossWalk.SOURCE_SYSTEM_CUSTOMER_ID.value().is(CUST_ID.get()))	
							.filter(CustCrossWalk.C_ADDRESSLINE1.value().is(ADDRESS_NAME.get()))
							//.collect(java.util.stream.Collectors.toList());	
							.findAny(); //find any one record to get the cust master id
					isSingleRec=true;	

				}//any 3
			}else if(CUST_NAME.isPresent() && CUST_ID.isPresent()&&CITY.isPresent()){
				try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){
					singleRec = 

							recordStream.filter(CustCrossWalk.NAME.value().is(CUST_NAME.get()))	
							.filter(CustCrossWalk.SOURCE_SYSTEM_CUSTOMER_ID.value().is(CUST_ID.get()))	
							.filter(CustCrossWalk.C_LOCALITY.value().is(CITY.get().toUpperCase()))
							//.collect(java.util.stream.Collectors.toList());	
							.findAny(); //find any one record to get the cust master id
					isSingleRec=true;	

				}//3
			}else if(CUST_NAME.isPresent()  && ADDRESS_NAME.isPresent() &&CITY.isPresent()){
				try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){
					multiRecs = 

							recordStream.filter(CustCrossWalk.NAME.value().is(CUST_NAME.get()))	
							.filter(CustCrossWalk.C_ADDRESSLINE1.value().is(ADDRESS_NAME.get()))
							.filter(CustCrossWalk.C_LOCALITY.value().is(CITY.get().toUpperCase()))
							.collect(java.util.stream.Collectors.toList());	

				}//3
			}else if(CUST_ID.isPresent() && ADDRESS_NAME.isPresent() &&CITY.isPresent()){
				try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){
					singleRec = 

							recordStream.filter(CustCrossWalk.NAME.value().is(CUST_NAME.get()))	
							.filter(CustCrossWalk.C_ADDRESSLINE1.value().is(ADDRESS_NAME.get()))
							.filter(CustCrossWalk.C_LOCALITY.value().is(CITY.get().toUpperCase()))
							//.collect(java.util.stream.Collectors.toList());	
							.findAny(); //find any one record to get the cust master id
					isSingleRec=true;	

				}//3
			}else if(CUST_NAME.isPresent() && CUST_ID.isPresent() ){
				try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){
					singleRec = 

							recordStream.filter(CustCrossWalk.NAME.value().is(CUST_NAME.get()))	
							.filter(CustCrossWalk.SOURCE_SYSTEM_CUSTOMER_ID.value().is(CUST_ID.get()))	
							//.collect(java.util.stream.Collectors.toList());	
							.findAny(); //find any one record to get the cust master id
					isSingleRec=true;	

				}//2
			}else if(CUST_NAME.isPresent() &&  ADDRESS_NAME.isPresent() ){
				try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){
					multiRecs = 

							recordStream.filter(CustCrossWalk.NAME.value().is(CUST_NAME.get()))								
							.filter(CustCrossWalk.C_ADDRESSLINE1.value().is(ADDRESS_NAME.get()))							
							.collect(java.util.stream.Collectors.toList());	

				}
			}else if(CUST_NAME.isPresent()&&CITY.isPresent()){
				try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){
					multiRecs = 

							recordStream.filter(CustCrossWalk.NAME.value().is(CUST_NAME.get()))	
							.filter(CustCrossWalk.C_LOCALITY.value().is(CITY.get().toUpperCase()))
							.collect(java.util.stream.Collectors.toList());	

				}
			}else if(CUST_ID.isPresent() && ADDRESS_NAME.isPresent() ){
				try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){
					singleRec = 

							recordStream.filter(CustCrossWalk.SOURCE_SYSTEM_CUSTOMER_ID.value().is(CUST_ID.get()))	
							.filter(CustCrossWalk.C_ADDRESSLINE1.value().is(ADDRESS_NAME.get()))
							//.collect(java.util.stream.Collectors.toList());	
							.findAny(); //find any one record to get the cust master id	
					isSingleRec=true;	

				}
			}else if( CUST_ID.isPresent()&&CITY.isPresent()){
				try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){
					singleRec = 

							recordStream.filter(CustCrossWalk.SOURCE_SYSTEM_CUSTOMER_ID.value().is(CUST_ID.get()))	
							.filter(CustCrossWalk.C_LOCALITY.value().is(CITY.get().toUpperCase()))
							//.collect(java.util.stream.Collectors.toList());	
							.findAny(); //find any one record to get the cust master id
					isSingleRec=true;	

				}
			}else if(ADDRESS_NAME.isPresent() &&CITY.isPresent()){
				try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){
					multiRecs = 

							recordStream	
							.filter(CustCrossWalk.C_ADDRESSLINE1.value().is(ADDRESS_NAME.get()))
							.filter(CustCrossWalk.C_LOCALITY.value().is(CITY.get().toUpperCase()))
							.collect(java.util.stream.Collectors.toList());	

				}
			}else if(CUST_ID.isPresent()){
				try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){
					singleRec = 
							recordStream.filter(CustCrossWalk.SOURCE_SYSTEM_CUSTOMER_ID.value().is(CUST_ID.get()))	
							//.collect(java.util.stream.Collectors.toList());	
							.findAny(); //find any one record to get the cust master id

					isSingleRec=true;	

				}
			}else if(CUST_NAME.isPresent()){
				try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){
					multiRecs = 

							recordStream.filter(CustCrossWalk.NAME.value().is(CUST_NAME.get()))	
							.collect(java.util.stream.Collectors.toList());	

				}
			}else if(ADDRESS_NAME.isPresent()){
				try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){
					multiRecs = 

							recordStream
							.filter(CustCrossWalk.C_ADDRESSLINE1.value().is(ADDRESS_NAME.get()))
							.collect(java.util.stream.Collectors.toList());	

				}
			}else if(CITY.isPresent()){ //bad performance
				try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){
					multiRecs = 		
							recordStream.filter(CustCrossWalk.C_LOCALITY.value().is(CITY.get().toUpperCase()))
							.collect(java.util.stream.Collectors.toList());	

				}
			}

			java.util.ArrayList<String> al = new java.util.ArrayList<String>();

			if(isSingleRec&&singleRec.isPresent()){
				al.add(singleRec.get().get(CustCrossWalk.CUSTOMER_ID).get());
			}else{
				if(multiRecs!=null)
					for(Record<String> record: multiRecs){
						al.add(record.get(CustCrossWalk.CUSTOMER_ID).get());
					}		

			}

			if(al.size()>0){
				pipelineCursor.insertAfter("MDM_CUST_ID", al.toArray());


			}
			long end_time = System.currentTimeMillis();
			pipelineCursor.insertAfter("responseTime", ""+(end_time-start_time));

		}catch(Exception e){
			e.printStackTrace(); //TBD. Should be deleted beforee golive
			IData	status = IDataFactory.create();
			IDataCursor statusCursor = status.getCursor();
			IDataUtil.put( statusCursor, "STATUS_CODE", "ERROR" );
			IDataUtil.put( statusCursor, "STATUS_MESG",e.getMessage() ); // TBD: need to replace with some business message
			statusCursor.destroy();
		}
		pipelineCursor.destroy();
	}
	public static BuildablePredicate<Record<?>>  addFilterPredicate( BuildablePredicate<Record<?>>  existPred, StringCellDefinition  cellDef,Optional<String> celVal){ 
		if(celVal.isPresent()){
			BuildablePredicate<Record<?>> newpred= cellDef.value().is(celVal.get());
			if(existPred!=null)  // add to existing predicate
				newpred = existPred.and(newpred);
			return newpred;
		}else{ //no filter value exists. donot add the predicate

			return existPred;	

		}

	}
	public static void printAllCrossWalk(String srcSystem,String customerID, String custName, String addressName, String City,String addressOnly){
		IData idata = IDataFactory.create();
		IDataCursor pipelineCursor = idata.getCursor();


		Optional<String> CUST_NAME = Optional.ofNullable(custName);
		Optional<String> CUST_ID =  Optional.ofNullable(customerID);
		Optional<String> ADDRESS_NAME =  Optional.ofNullable(addressName);
		Optional<String> SRC_SYSTEM =  Optional.ofNullable(srcSystem);
		Optional<String> CITY =  Optional.ofNullable(City);
		Optional<String> singleAddressOnly =  Optional.ofNullable(addressOnly);

		java.util.List<Record<String>> multiRecs = null ;
		Optional<Record<String>> singleRec = null ;
		boolean isSingleRec =false;
		//long start_time = System.nanoTime();
		long start_time = System.currentTimeMillis();
		try(Dataset<String> crossWalkDS = com.gcs.tcclient.DataSetManagerFactory.getCustCrossWalkDataSet();){
			//com.gcs.tcclient.DataSetManagerFactory.createCustCrossWalkDataSet();

			DatasetWriterReader<String> crosswalkReader = crossWalkDS.writerReader();
			try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){
				// add customer id to filter
				BuildablePredicate<Record<?>>  filterPredicate = addFilterPredicate(null,CustCrossWalk.SOURCE_SYSTEM_CUSTOMER_ID,CUST_ID);
				// add  Src system to filter 
				filterPredicate = addFilterPredicate(filterPredicate,CustCrossWalk.SOURCE_SYSTEM,SRC_SYSTEM);

				filterPredicate = addFilterPredicate(filterPredicate,CustCrossWalk.NAME,CUST_NAME);
				filterPredicate = addFilterPredicate(filterPredicate,CustCrossWalk.C_ADDRESSLINE1,ADDRESS_NAME);
				filterPredicate = addFilterPredicate(filterPredicate,CustCrossWalk.C_LOCALITY,CITY);

				if(filterPredicate!=null){
					
						multiRecs = 						
								recordStream.batch(50)
								//.explain(System.out::println)
								.filter(filterPredicate)
								.collect(java.util.stream.Collectors.toList());	

					
				}else{
					multiRecs = 						
							recordStream.batch(50)
							//.explain(System.out::println)

							.collect(java.util.stream.Collectors.toList());

				}
				java.util.HashSet<String> custIdList = new java.util.HashSet<String>();
				java.util.Hashtable<String,Record<?>> custHashTb = new java.util.Hashtable<String,Record<?>>();

				if(isSingleRec&&singleRec.isPresent()){
					custIdList.add(singleRec.get().get(CustCrossWalk.CUSTOMER_ID).get());
				}else{
					if(multiRecs!=null)
						for(Record<String> record: multiRecs){
							String k1 = record.get(CustCrossWalk.CUSTOMER_ID).get();
							custIdList.add(k1);
							custHashTb.put(k1, record);

						}		

				}
				int i=0;
				for(Record<?> record : custHashTb.values())
					System.out.println(record.get(CustCrossWalk.SOURCE_SYSTEM).get()+","+
							record.get(CustCrossWalk.SOURCE_SYSTEM_CUSTOMER_ID).get()+","+
							record.get(CustCrossWalk.NAME).get()+","+
							record.get(CustCrossWalk.SOURCE_SYSTEM_CUSTOMER_ID).get()+","+
							record.get(CustCrossWalk.CUSTOMER_ID).get()+",");




			}
		}catch(Exception e){

		}
		pipelineCursor.destroy();


	}
	public static IData getCustomerIDataByKey(DatasetReader custReader,String custID ){
		IData	master = IDataFactory.create();
		IDataCursor masterCursor = master.getCursor();
		Optional<Record<Long>> custRec = custReader.get(Long.parseLong(custID));
		IDataUtil.put( masterCursor, "ID",  custRec.get().getKey() );
		IDataUtil.put( masterCursor, "CUST_NAME", custRec.get().get(CustMaster.CUST_NAME).get());
		IDataUtil.put( masterCursor, "CUST_LNG_NAME", custRec.get().get(CustMaster.CUST_LNG_NAME).get());
		IDataUtil.put( masterCursor, "CUST_SHRT_NAME", custRec.get().get(CustMaster.CUST_SHRT_NAME).get());
		IDataUtil.put( masterCursor, "CUST_SURNM1", custRec.get().get(CustMaster.CUST_SURNM1).get());
		IDataUtil.put( masterCursor, "CUST_SURNM2", custRec.get().get(CustMaster.CUST_SURNM2).get());
		//IDataUtil.put( masterCursor, "SURVIVORSHIP_DETAILS", "SURVIVORSHIP_DETAILS" );
		masterCursor.destroy();
		return master;
	}
	public static java.util.Hashtable<Long,IData> getServicesIDataByAddressId(String custID,String CUST_ADDR_ID) throws Exception{
		//getservices for each address
		Dataset<Long> servicesDS = com.gcs.tcclient.DataSetManagerFactory.getServiceDataSet();
		DatasetWriterReader<Long> servicesReader = servicesDS.writerReader();	
		java.util.Hashtable<Long,IData> servicesAL=new java.util.Hashtable<Long,IData>();
		try(MutableRecordStream<Long> servicesStream = 	servicesReader.records()){
			// get all adddressf or the given customerID.
			java.util.List<Record<Long>> serviceRecs = 		
					servicesStream.explain(System.out::println)
					.filter(CustService.CUSTOMER_ID.value().is(custID))
					.filter(CustService.CUST_ADDR_ID.value().is(CUST_ADDR_ID))
					.collect(java.util.stream.Collectors.toList());				
			for(Record<Long> svcRec: serviceRecs){
				IData	service = IDataFactory.create();
				IDataCursor serviceCursor = service.getCursor();
				IDataUtil.put( serviceCursor, "SRVC_ID",svcRec.getKey()+"" );
				if(svcRec.get(CustService.SERVICE_TYPE).isPresent()) 
					IDataUtil.put( serviceCursor, "SRVC_TYP", svcRec.get(CustService.SERVICE_TYPE).get() );
				if(svcRec.get(CustService.SERVICE_TYPE_CODE).isPresent()) 
					IDataUtil.put( serviceCursor, "SRVC_TYP_CD",svcRec.get(CustService.SERVICE_TYPE_CODE).get() );
				if(svcRec.get(CustService.SERVICE_SOURCE_SYSTEM).isPresent())
					IDataUtil.put( serviceCursor, "SRC_SYS_ID", svcRec.get(CustService.SERVICE_SOURCE_SYSTEM).get() );
				IDataUtil.put( serviceCursor, "SRC_SYS_SRV_CD", svcRec.get(CustService.SOURCE_SYSTEM_SERVICE_ID).get() );
				IDataUtil.put( serviceCursor, "SRVC_CD", svcRec.get(CustService.SERVICE_CODE).get() );
				IDataUtil.put( serviceCursor, "SRVC_DESC", svcRec.get(CustService.SERVICE_DESCRIPTION).get() );
				if(svcRec.get(CustService.READY_RECKONER).isPresent())
					IDataUtil.put( serviceCursor, "READY_RECKONER",svcRec.get(CustService.READY_RECKONER).get() );
				if(svcRec.get(CustService.INSTALLED_INDICATOR).isPresent())
					IDataUtil.put( serviceCursor, "INSTALLED_INDICATOR", svcRec.get(CustService.INSTALLED_INDICATOR).get() );
				if(svcRec.get(CustService.CONFIDENTIAL_INDICATOR).isPresent())
					IDataUtil.put( serviceCursor, "CONFIDENTIAL_INDICATOR", svcRec.get(CustService.CONFIDENTIAL_INDICATOR).get() );
				if(svcRec.get(CustService.FAMILY_SERVICE_CODE).isPresent())
					IDataUtil.put( serviceCursor, "FAMILY_SERVICE_CODE", svcRec.get(CustService.FAMILY_SERVICE_CODE).get() );
				if(svcRec.get(CustService.CATALOG_SERVICE_CODE).isPresent())
					IDataUtil.put( serviceCursor, "CATALOG_SERVICE_CODE",svcRec.get(CustService.CATALOG_SERVICE_CODE).get() );
				if(svcRec.get(CustService.CATALOG_INDICATOR).isPresent())
					IDataUtil.put( serviceCursor, "CATALOG_INDICATOR",svcRec.get(CustService.CATALOG_INDICATOR).get() );
				if(svcRec.get(CustService.MODIFY_USER).isPresent())
					IDataUtil.put( serviceCursor, "MODIFY_USR", svcRec.get(CustService.MODIFY_USER).get() );
				if(svcRec.get(CustService.SERVICE_START_DATE).isPresent())
					IDataUtil.put( serviceCursor, "SRVC_STRT_DT", svcRec.get(CustService.SERVICE_START_DATE).get() );
				if(svcRec.get(CustService.SERVICE_END_DATE).isPresent())
					IDataUtil.put( serviceCursor, "SRVC_END_DT", svcRec.get(CustService.SERVICE_END_DATE).get() );
				if(svcRec.get(CustService.MODIFY_DATE).isPresent())
					IDataUtil.put( serviceCursor, "MODIFY_DT", svcRec.get(CustService.MODIFY_DATE).get() );
				if(svcRec.get(CustService.MODIFY_USER).isPresent())
					IDataUtil.put( serviceCursor, "CNTRCT_ID",svcRec.get(CustService.MODIFY_USER).get() );
				serviceCursor.destroy();
				servicesAL.put(svcRec.getKey(),service);
			}
		}
		return servicesAL;
	}
	public static IData createStatusRecord(String ERR_CODE,String ERR_MESG){
		IData	status = IDataFactory.create();
		IDataCursor statusCursor = status.getCursor();
		IDataUtil.put( statusCursor, "STATUS_CODE", ERR_CODE);
		IDataUtil.put( statusCursor, "STATUS_MESG", ERR_MESG );
		statusCursor.destroy();
		return status;
	}
	public static IDataCursor createAddressIData(IDataCursor addressCursor ,String CUST_ADDR_ID,Record<Long> addrRec ){
		IDataUtil.put( addressCursor, "CUST_ADDR_ID",""+CUST_ADDR_ID );
		IDataUtil.put( addressCursor, "ADDRESSLINE1", addrRec.get(CustAddress.ADDRESSLINE1).get() );
		IDataUtil.put( addressCursor, "ADDRESSLINE2", addrRec.get(CustAddress.ADDRESSLINE2).get() );
		IDataUtil.put( addressCursor, "ADMINISTRATIVEAREA", addrRec.get(CustAddress.ADMINISTRATIVEAREA).get() );
		IDataUtil.put( addressCursor, "LOCALITY", addrRec.get(CustAddress.LOCALITY).get() );
		IDataUtil.put( addressCursor, "POSTALCODE",  addrRec.get(CustAddress.POSTALCODE).get() );
		IDataUtil.put( addressCursor, "COUNTRY",  addrRec.get(CustAddress.COUNTRY).get() );
		IDataUtil.put( addressCursor, "CUST_ID",  addrRec.get(CustAddress.CUST_ID).get() );
		IDataUtil.put( addressCursor, "LONGITUDE",  addrRec.get(CustAddress.LONGITUDE).get() );
		IDataUtil.put( addressCursor, "LATTITUDE",  addrRec.get(CustAddress.LATTITUDE).get() );
		if(addrRec.get(CustAddress.CUST_ADDR_EMAIL).isPresent())
			IDataUtil.put( addressCursor, "CUST_ADDR_EMAIL",  addrRec.get(CustAddress.CUST_ADDR_EMAIL).get() );
		if(addrRec.get(CustAddress.CUST_ADDR_PHONE_NUMBER).isPresent())
			IDataUtil.put( addressCursor, "CUST_ADDR_PHONE_NUMBER", addrRec.get(CustAddress.CUST_ADDR_PHONE_NUMBER).get() );
		if(addrRec.get(CustAddress.CUST_ADDR_FAX_NUMBER).isPresent())
			IDataUtil.put( addressCursor, "CUST_ADDR_FAX_NUMBER",  addrRec.get(CustAddress.CUST_ADDR_FAX_NUMBER).get() );
		IDataUtil.put( addressCursor, "CUST_ADDR_TYPE", addrRec.get(CustAddress.CUST_ADDR_TYPE).get() );
		return addressCursor;
	}
	public static java.util.Hashtable<Long,IData> getAddressIDataByCustomerId(java.util.Hashtable<String,java.util.HashSet<String>> custIDAddressIds,Optional<String> singleAddressOnly,Optional<String> ADDRESS_NAME,String custID)  throws Exception{

		java.util.Hashtable<Long,IData> addressAL=new java.util.Hashtable<Long,IData>();
		Dataset<Long> addressDS = com.gcs.tcclient.DataSetManagerFactory.getAddressDataSet();
		DatasetWriterReader<Long> addressReader = addressDS.writerReader();
	
		if(custIDAddressIds != null && custIDAddressIds.size() > 0 && (custIDAddressIds.get(custID) != null)){
				java.util.HashSet<String> addresIdList = custIDAddressIds.get(custID);
				for(String addrId :addresIdList){
					Optional<Record<Long>> addRec = addressReader.get(Long.parseLong(addrId));
					if(addRec.isPresent()){
						IData	address = IDataFactory.create();
						IDataCursor addressCursor = address.getCursor();					
						addressCursor = createAddressIData( addressCursor ,addrId, addRec.get() );
						//now get all the services that belong
						java.util.Hashtable<Long,IData> services =			 getServicesIDataByAddressId( custID, addrId);
						if(services.size()>0)//insert services to each address
							IDataUtil.put(addressCursor,"service",services.values().toArray(new IData[services.size()]));			
						addressCursor.destroy();					
						addressAL.put(Long.parseLong(addrId), address);
					}
				}
		}else{
				try(MutableRecordStream<Long> addressStream = 	addressReader.records()){
				// get all adddressf or the given customerID.
				boolean singleAddress =false;

				if(singleAddressOnly.isPresent() && singleAddressOnly.get().equals("true")){
					singleAddress=true;
				}
				BuildablePredicate<Record<?>>  filterPredicate = addFilterPredicate(null,CustAddress.CUST_ID, Optional.ofNullable(custID));


				if(singleAddress && ADDRESS_NAME.isPresent())
					filterPredicate = addFilterPredicate(filterPredicate,CustAddress.ADDRESSLINE1,ADDRESS_NAME);

				java.util.List<Record<Long>> addressRecs = addressStream.filter(filterPredicate).collect(java.util.stream.Collectors.toList());			
				if(addressRecs!=null){			

					for(Record<Long> addrRec: addressRecs){					
						IData	address = IDataFactory.create();
						IDataCursor addressCursor = address.getCursor();
						String CUST_ADDR_ID = ""+addrRec.getKey();
						
						addressCursor = createAddressIData( addressCursor ,CUST_ADDR_ID, addrRec );
						//now get all the services that belong
						java.util.Hashtable<Long,IData> services =			 getServicesIDataByAddressId( custID, CUST_ADDR_ID);
						if(services.size()>0)//insert services to each address
							IDataUtil.put(addressCursor,"service",services.values().toArray(new IData[services.size()]));			
						addressCursor.destroy();					
						addressAL.put(addrRec.getKey(), address);
					}
				}
			}
		}
		return addressAL;


	}
	public static void get360(String srcSystem,String customerID, String custName, String addressName, String City,String addressOnly) throws Exception{
		IData pipeline = IDataFactory.create();
		IDataCursor pipelineCursor = pipeline.getCursor();

		// pipeline


		/*
				Optional<String> CUST_NAME = Optional.ofNullable(null);//IDataUtil.getString( pipelineCursor, "CUST_NAME" ));
				Optional<String> CUST_ID =  Optional.ofNullable(""+17477911); //66032589
				Optional<String> ADDRESS_NAME = Optional.ofNullable(null);
				Optional<String> CITY = Optional.ofNullable(null);
		 */
		Optional<String> CUST_NAME = Optional.ofNullable(custName);
		Optional<String> CUST_ID =  Optional.ofNullable(customerID);
		Optional<String> ADDRESS_NAME =  Optional.ofNullable(addressName);
		Optional<String> SRC_SYSTEM =  Optional.ofNullable(srcSystem);
		Optional<String> CITY =  Optional.ofNullable(City);
		Optional<String> singleAddressOnly =  Optional.ofNullable(addressOnly);

		java.util.List<Record<String>> multiRecs = null ;
		Optional<Record<String>> singleRec = null ;
		boolean isSingleRec =false;
		//long start_time = System.nanoTime();
		long start_time = System.currentTimeMillis();
		try(Dataset<String> crossWalkDS = com.gcs.tcclient.DataSetManagerFactory.getCustCrossWalkDataSet();){

			DatasetWriterReader<String> crosswalkReader = crossWalkDS.writerReader();
			try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){
				// add Filters
				BuildablePredicate<Record<?>>  filterPredicate = addFilterPredicate(null,CustCrossWalk.SOURCE_SYSTEM_CUSTOMER_ID,CUST_ID);
				filterPredicate = addFilterPredicate(filterPredicate,CustCrossWalk.SOURCE_SYSTEM,SRC_SYSTEM);
				filterPredicate = addFilterPredicate(filterPredicate,CustCrossWalk.NAME,CUST_NAME);
				filterPredicate = addFilterPredicate(filterPredicate,CustCrossWalk.C_ADDRESSLINE1,ADDRESS_NAME);
				filterPredicate = addFilterPredicate(filterPredicate,CustCrossWalk.C_LOCALITY,CITY);

				if(filterPredicate!=null){						
					multiRecs = 						
							recordStream.batch(50)
							.explain(System.out::println)
							.filter(filterPredicate)
							.collect(java.util.stream.Collectors.toList());					

				}else{
					multiRecs = 						
							recordStream.batch(50)
							.explain(System.out::println)
							.collect(java.util.stream.Collectors.toList());

				}
			} // end of reading the data from crsoss walk
			java.util.HashSet<String> custIdList = new java.util.HashSet<String>();
			java.util.Hashtable<String,java.util.HashSet<String>> custIDAddressIds = new java.util.Hashtable<String,java.util.HashSet<String>>();
			if(multiRecs!=null)
				for(Record<String> record: multiRecs){
					String custMastId=record.get(CustCrossWalk.CUSTOMER_ID).get();
					custIdList.add(custMastId);
					java.util.HashSet<String> hs =  custIDAddressIds.get(custMastId);
					if(hs==null){
						hs = new java.util.HashSet<String>();
						custIDAddressIds.put(custMastId,hs);
					}
					hs.add(record.get(CustCrossWalk.ADDRESS_ID).get())	;							

				}		



			//Now get all the customer for the given custId list				

			// customers.customer
			java.util.ArrayList<IData>	customerAL = new java.util.ArrayList<IData>();
			if(custIdList.size()>0){
				IData	customers = IDataFactory.create();
				IDataCursor customersCursor = customers.getCursor();
				try(Dataset<Long> custDS = com.gcs.tcclient.DataSetManagerFactory.getCustMasterDataSet();){
					for(String custID: custIdList){		
						DatasetReader<Long> custReader = custDS.reader();	
						IData customer = IDataFactory.create();
						IDataCursor customerCursor = customer.getCursor();							
						IDataUtil.put(customerCursor,"master",getCustomerIDataByKey(custReader,custID));
						//now get the Addresses
						java.util.Hashtable<Long,IData>  addressAL = getAddressIDataByCustomerId( custIDAddressIds,singleAddressOnly,ADDRESS_NAME,custID) ;

						if(addressAL.size()>0)//insert Addresses
							IDataUtil.put(customerCursor,"address",addressAL.values().toArray(new IData[addressAL.size()]));
						customerCursor.destroy();
						customerAL.add(customer);
					}
					if(customerAL.size()>0){
						IDataUtil.put(customersCursor,"customer",customerAL.toArray(new IData[customerAL.size()]));
						IDataUtil.put( pipelineCursor, "customers", customers );
						IDataUtil.put( pipelineCursor, "status", createStatusRecord("OK","Successfully retrieved "+customerAL.size()+" customers.") );
					}else{
						IDataUtil.put( pipelineCursor, "status", createStatusRecord("ERROR","No customer records found for the given search criteria") );
					}
					//long end_time = System.nanoTime();
					customersCursor.destroy();
					long end_time = System.currentTimeMillis();




				}catch(Exception e){
					e.printStackTrace(); //TBD. Should be deleted beforee golive
					IDataUtil.put( pipelineCursor, "status", createStatusRecord("ERROR",e.getMessage()));

				}

				pipelineCursor.destroy();
				printPipeline(pipeline,"");
			}
		}



	}
	public static void print(String prefix,String key,Object obj){
		System.out.println(prefix+" "+key+ " : " + obj);
	}
	public static void printPipeline(IData pipeline,String prefix){

		IDataCursor idc = pipeline.getCursor();
		if(idc.first()){
			do{

				Object val = idc.getValue();
				if(val!=null){
					if (val   instanceof com.wm.data.ISMemDataImpl){
						System.out.println(prefix+"-->"+idc.getKey() );
						printPipeline((IData)val,prefix+"-->");
					}else if (val   instanceof Long)
						print(prefix,idc.getKey(),idc.getValue());
					else if (val   instanceof String)
						print(prefix,idc.getKey(),idc.getValue());
					else if(val.getClass().isArray()){
						Object[] objs = IDataUtil.getObjectArray(idc, idc.getKey());
						;
						for(Object obj : objs){	
							System.out.println(prefix+"-->"+idc.getKey() );
							printPipeline((IData)obj,prefix+"-->");
							//System.out.println(idc.getKey() + "  ---   " + obj.getClass().getName());
						}


					}else{
						System.out.println(idc.getKey() + "  ---   " + idc.getValue().getClass().getName());
					}
				}
			} while(idc.next());

		}
		idc.destroy();

	}

	public static void destroy(){


	}
	public static void delete(String[] args){

		long start_time = System.currentTimeMillis();
		try{
			com.gcs.tcclient.DataSetManagerFactory.createCustCrossWalkDataSet();
			Dataset<String> crossWalkDS = com.gcs.tcclient.DataSetManagerFactory.getCustCrossWalkDataSet();
			DatasetWriterReader<String> crosswalkReader = crossWalkDS.writerReader();

			try(MutableRecordStream<String> recordStream = 	crosswalkReader.records()){

				java.util.List<Record<String>> multiRecs = 

						recordStream	.collect(java.util.stream.Collectors.toList());	
				if(multiRecs!=null)
					for(Record<String> record: multiRecs){
						crosswalkReader.delete(record.getKey());

					}		

			}//3


			Dataset<Long> custDS = com.gcs.tcclient.DataSetManagerFactory.getCustMasterDataSet();
			//  Dataset<String> ds = getDatasetFromStack(); 

			DatasetWriterReader<Long> custReader = custDS.writerReader();	
			try(MutableRecordStream<Long> recordStream = 	custReader.records()){
				java.util.List<Record<Long>> multiRecs =								
						recordStream	.collect(java.util.stream.Collectors.toList());	
				if(multiRecs!=null)
					for(Record<Long> record: multiRecs){
						custReader.delete(record.getKey());

					}		

			}


			Dataset<Long> addresssDS = com.gcs.tcclient.DataSetManagerFactory.getAddressDataSet();
			//  Dataset<String> ds = getDatasetFromStack(); 

			DatasetWriterReader<Long> addresReader = addresssDS.writerReader();	
			try(MutableRecordStream<Long> recordStream = 	addresReader.records()){
				java.util.List<Record<Long>> multiRecs =								
						recordStream	.collect(java.util.stream.Collectors.toList());	
				if(multiRecs!=null)
					for(Record<Long> record: multiRecs){
						addresReader.delete(record.getKey());

					}		

			}

			Dataset<Long> serviceDS = com.gcs.tcclient.DataSetManagerFactory.getServiceDataSet();
			//  Dataset<String> ds = getDatasetFromStack(); 

			DatasetWriterReader<Long> serviceReader = serviceDS.writerReader();	
			try(MutableRecordStream<Long> recordStream = 	serviceReader.records()){
				java.util.List<Record<Long>> multiRecs =								
						recordStream	.collect(java.util.stream.Collectors.toList());	
				if(multiRecs!=null)
					for(Record<Long> record: multiRecs){
						serviceReader.delete(record.getKey());

					}		

			}
		}catch(Exception e){
			e.printStackTrace();
		}

	}
}


