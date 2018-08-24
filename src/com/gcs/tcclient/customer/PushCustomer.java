package com.gcs.tcclient.customer;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Type;
import com.terracottatech.store.async.AsyncDatasetWriterReader;
import com.terracottatech.store.async.Operation;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.manager.DatasetManager;


import java.util.concurrent.TimeUnit;

import com.terracottatech.store.Cell;




public class PushCustomer {
	private static final String STORE_NAME = "customers";

	private static final StringCellDefinition FNAME = CellDefinition.defineString("firstName");
	private static final StringCellDefinition LNAME = CellDefinition.defineString("lastName");
	private static final StringCellDefinition EMAIL = CellDefinition.defineString("email");
	private static final StringCellDefinition MPHONE = CellDefinition.defineString("mobilePhone");
	private static final StringCellDefinition HPHONE = CellDefinition.defineString("homePhone");
	private static final StringCellDefinition ADDR1 = CellDefinition.defineString("address1");
	private static final StringCellDefinition ADDR2 = CellDefinition.defineString("address2");
	private static final StringCellDefinition CITY = CellDefinition.defineString("city");
	private static final StringCellDefinition ST = CellDefinition.defineString("state");
	private static final StringCellDefinition ZIP = CellDefinition.defineString("zip");
	public static void main(String argsp[]) throws Exception{

		java.net.URI clusterUri = new java.net.URI( "terracotta://daehgcs28836.daedmz.loc:9410");
		try ( 
				//System.out.println
				DatasetManager datasetManager =
				DatasetManager.clustered(clusterUri).withConnectionTimeout(300,TimeUnit.SECONDS).withReconnectTimeout(300,TimeUnit.SECONDS) // <1>
				.build()) { // <2>

			DatasetConfiguration customerConfig =
					datasetManager.datasetConfiguration() // <3>
					.offheap("second") // <4>
					.disk("customer")
					.index(FNAME,  IndexSettings.BTREE)// <5>
					.index(LNAME,IndexSettings.BTREE)
					.index(MPHONE,IndexSettings.BTREE)
					.build(); // <6>

			//datasetManager.destroyDataset(STORE_NAME);


			datasetManager.newDataset(STORE_NAME, Type.STRING, customerConfig); // <7>
			try (Dataset<String> customers =
					datasetManager.getDataset("customers", Type.STRING)) { // <8>
				// Use the Dataset
				long l = 1,p=1111111111; 
				int z=10000;

				DatasetWriterReader<String> writerReader = customers.writerReader();
				// AsyncDatasetWriterReader<String> asyncAccess 		 = writerReader.async(); // <1>

				Cell<String>  firstName,lastName, email,mobilePhone,homePhone,address1,address2,city,state,zip;
				java.util.Date start = new java.util.Date();
				long start_time = System.nanoTime();


				System.out.println();
				for(int i=0;i<5000000;i++){
					// writerReader.delete(""+i);
					firstName = FNAME.newCell("Prudhvi"+l);
					//lastName = Cell.cell("lastName", "Penmetsa"+l);
					lastName = LNAME.newCell("Penmetsa"+l);
					email = EMAIL.newCell("Prudhvi"+l+"@gmail.com");
					mobilePhone = MPHONE.newCell(""+p++);
					homePhone = HPHONE.newCell(""+p++);
					address1 = ADDR1.newCell( l+" No Street");
					address2 = ADDR2.newCell("None");
					city =CITY.newCell( "Jacksonville");
					state = ST.newCell( "FL");
					zip = ZIP.newCell( ""+z++);

					writerReader.add(""+l++,firstName,lastName, email,mobilePhone,homePhone,address1,address2,city,state,zip);
					// Operation<Boolean> addOp = asyncAccess.add(""+l++,firstName,lastName, email,mobilePhone,homePhone,address1,address2,city,state,zip);


					if(z % 20000 == 0){
						long end_time = System.nanoTime();
						double difference = (end_time - start_time) / 1e6;
						System.out.println( l + " reccords inserted in  " +difference + "milli seconds");
					}

				}
				System.out.println(new java.util.Date());
				System.out.println( l + " Reccords inserted");
			}catch(Exception e2){
				e2.printStackTrace();
			}
		}catch(Exception e1){
			e1.printStackTrace();

		}

	}

}
