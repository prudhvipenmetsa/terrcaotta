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
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.stream.MutableRecordStream;
import com.wm.data.*;

public class GetCustomer360 {
	public static void main(String[] args){
		IData idata = IDataFactory.create();
		IDataCursor pipelineCursor = idata.getCursor();

		DataSetManagerFactory.tcURL= "terracotta://daehgcs28835.daedmz.loc:9410";
		Optional<String> CUST_NAME = Optional.ofNullable(null);//IDataUtil.getString( pipelineCursor, "CUST_NAME" ));
		Optional<String> CUST_ID =  Optional.ofNullable(""+17477911); //66032589
		Optional<String> ADDRESS_NAME = Optional.ofNullable(null);
		Optional<String> CITY = Optional.ofNullable(null);
		java.util.List<Record<String>> multiRecs = null ;
		Optional<Record<String>> singleRec = null ;
		boolean isSingleRec =false;
		//long start_time = System.nanoTime();
		long start_time = System.currentTimeMillis();
		try{
			com.gcs.tcclient.DataSetManagerFactory.createCustCrossWalkDataSet();
			Dataset<String> crossWalkDS = com.gcs.tcclient.DataSetManagerFactory.getCustCrossWalkDataSet();
			DatasetWriterReader<String> crosswalkReader = crossWalkDS.writerReader();

			if(CUST_NAME.isPresent() && CUST_ID.isPresent() && ADDRESS_NAME.isPresent() &&CITY.isPresent()){
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

			//get all customers
			// customers
			IData	customers = IDataFactory.create();
			IDataCursor customersCursor = customers.getCursor();

			// customers.customer
			java.util.ArrayList<IData>	customerAL = new java.util.ArrayList<IData>();
			if(al.size()>0)


				for(String custID: al){
					Dataset<Long> custDS = com.gcs.tcclient.DataSetManagerFactory.getCustMasterDataSet();
					//  Dataset<String> ds = getDatasetFromStack(); 

					DatasetReader<Long> custReader = custDS.reader();				
					
					Optional<Record<Long>> custRec = custReader.get(Long.parseLong(custID));
					IData customer = IDataFactory.create();
					IDataCursor customerCursor = customer.getCursor();
					IData	master = IDataFactory.create();
					IDataCursor masterCursor = master.getCursor();
					IDataUtil.put( masterCursor, "ID",  custRec.get().getKey() );
					IDataUtil.put( masterCursor, "CUST_NAME", custRec.get().get(CustMaster.CUST_NAME).get());
					IDataUtil.put( masterCursor, "CUST_LNG_NAME", custRec.get().get(CustMaster.CUST_LNG_NAME).get());
					IDataUtil.put( masterCursor, "CUST_SHRT_NAME", custRec.get().get(CustMaster.CUST_SHRT_NAME).get());
					IDataUtil.put( masterCursor, "CUST_SURNM1", custRec.get().get(CustMaster.CUST_SURNM1).get());
					IDataUtil.put( masterCursor, "CUST_SURNM2", custRec.get().get(CustMaster.CUST_SURNM2).get());
					//IDataUtil.put( masterCursor, "SURVIVORSHIP_DETAILS", "SURVIVORSHIP_DETAILS" );
					masterCursor.destroy();
					IDataUtil.put(customerCursor,"master",master);
					//now get the Addresses
					Dataset<Long> addressDS = com.gcs.tcclient.DataSetManagerFactory.getAddressDataSet();
					DatasetWriterReader<Long> addressReader = addressDS.writerReader();
					java.util.List<Record<Long>> addressRecs = null ;


					try(MutableRecordStream<Long> addressStream = 	addressReader.records()){
						// get all adddressf or the given customerID.
						addressRecs = 		
								addressStream.filter(CustAddress.CUST_ID.value().is(custID))						
								.collect(java.util.stream.Collectors.toList());	
						java.util.ArrayList<IData> addressAL=new java.util.ArrayList<IData>();
						for(Record<Long> addrRec: addressRecs){
							IData	address = IDataFactory.create();
							IDataCursor addressCursor = address.getCursor();
							String CUST_ADDR_ID = ""+addrRec.getKey();
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
							//IDataUtil.put( addressCursor, "CUST_ADDR_EMAIL",  addrRec.get(CustAddress.CUST_ADDR_EMAIL).get() );
							//IDataUtil.put( addressCursor, "CUST_ADDR_PHONE_NUMBER", addrRec.get(CustAddress.CUST_ADDR_PHONE_NUMBER).get() );
							//IDataUtil.put( addressCursor, "CUST_ADDR_FAX_NUMBER",  addrRec.get(CustAddress.CUST_ADDR_FAX_NUMBER).get() );
							//IDataUtil.put( addressCursor, "WINKEY_ADDR", "WINKEY_ADDR" );
							//IDataUtil.put( addressCursor, "WINKEY_EMAIL", "WINKEY_EMAIL" );
							//IDataUtil.put( addressCursor, "WINKEY_PHNMBR", "WINKEY_PHNMBR" );
							//IDataUtil.put( addressCursor, "WINKEY_FXNMBR", "WINKEY_FXNMBR" );
							IDataUtil.put( addressCursor, "CUST_ADDR_TYPE", addrRec.get(CustAddress.CUST_ADDR_TYPE).get() );

							addressAL.add(address);
							//getservices for each address
							Dataset<Long> servicesDS = com.gcs.tcclient.DataSetManagerFactory.getServiceDataSet();
							DatasetWriterReader<Long> servicesReader = servicesDS.writerReader();


							try(MutableRecordStream<Long> servicesStream = 	servicesReader.records()){
								// get all adddressf or the given customerID.
								java.util.List<Record<Long>> serviceRecs = 		
										servicesStream.filter(CustService.CUSTOMER_ID.value().is(custID))
										.filter(CustService.CUST_ADDR_ID.value().is(CUST_ADDR_ID))
										.collect(java.util.stream.Collectors.toList());	
								java.util.ArrayList<IData> servicesAL=new java.util.ArrayList<IData>();
								for(Record<Long> svcRec: serviceRecs){
									IData	service = IDataFactory.create();
									IDataCursor serviceCursor = service.getCursor();

									IDataUtil.put( serviceCursor, "SRVC_ID",svcRec.getKey()+"" );
									if(svcRec.get(CustService.SERVICE_TYPE).isPresent()) 
										IDataUtil.put( serviceCursor, "SRVC_TYP", svcRec.get(CustService.SERVICE_TYPE).get() );
									if(svcRec.get(CustService.SERVICE_TYPE_CODE).isPresent()) 
									IDataUtil.put( serviceCursor, "SRVC_TYP_CD",svcRec.get(CustService.SERVICE_TYPE_CODE).get() );
									if(svcRec.get(CustService.SOURCE_SYSTEM_SERVICE_ID).isPresent())
										IDataUtil.put( serviceCursor, "SRC_SYS_ID", svcRec.get(CustService.SOURCE_SYSTEM_SERVICE_ID).get() );
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

									servicesAL.add(service);
									//getservices for each address

								}
								if(servicesAL.size()>0)//insert Addresses
									IDataUtil.put(addressCursor,"service",servicesAL.toArray(new IData[servicesAL.size()]));			
							}
							addressCursor.destroy();
						}

						if(addressAL.size()>0)//insert Addresses
							IDataUtil.put(customerCursor,"address",addressAL.toArray(new IData[addressAL.size()]));			
					}


					customerCursor.destroy();
					customerAL.add(customer);
				}

			IDataUtil.put(customersCursor,"customer",customerAL.toArray(new IData[customerAL.size()]));
			//long end_time = System.nanoTime();
			customersCursor.destroy();
			long end_time = System.currentTimeMillis();
			IDataUtil.put( pipelineCursor, "customers", customers );
			// status
			IData	status = IDataFactory.create();
			IDataCursor statusCursor = status.getCursor();
			IDataUtil.put( statusCursor, "STATUS_CODE", "OK" );
			IDataUtil.put( statusCursor, "STATUS_MESG", "Successfully retrieved the data" );
			statusCursor.destroy();
			IDataUtil.put( pipelineCursor, "status", status );

		}catch(Exception e){
			e.printStackTrace();
		}
		pipelineCursor.destroy();
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


