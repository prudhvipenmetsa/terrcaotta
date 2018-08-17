package com.gcs.tcclient.customer;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Type;
import com.terracottatech.store.async.AsyncDatasetWriterReader;
import com.terracottatech.store.async.Operation;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.Cell;




public class PushCustomer2 {

	public static void main(String argsp[]) throws Exception{

		java.net.URI clusterUri = new java.net.URI( "terracotta://daehgcs28835.daedmz.loc:9410");
		try ( 
				DatasetManager datasetManager =
				DatasetManager.clustered(clusterUri) // <1>
				.build()) { // <2>

			DatasetConfiguration customerConfig =
					datasetManager.datasetConfiguration() // <3>
					.offheap("second") // <4>
					.disk("customer") // <5>
					.build(); // <6>
			datasetManager.newDataset("customers", Type.STRING, customerConfig); // <7>
			try (Dataset customers =
					datasetManager.getDataset("customers", Type.STRING)) { // <8>
				// Use the Dataset
				long l = 5000001,p=1111111111; 
				int z=10000;

				DatasetWriterReader<String> writerReader = customers.writerReader();
				 AsyncDatasetWriterReader<String> asyncAccess 		 = writerReader.async(); // <1>

				Cell<String>  firstName,lastName, email,mobilePhone,homePhone,address1,address2,city,state,zip;
				java.util.Date start = new java.util.Date();
				long start_time = System.nanoTime();
				
		
				System.out.println();
				for(int i=0;i<5000000;i++){
					firstName = Cell.cell("firstName", "Prudhvi"+l);
					lastName = Cell.cell("lastName", "Penmetsa"+l);
					email = Cell.cell("email", "Prudhvi"+l+"@gmail.com");
					mobilePhone = Cell.cell("mobilePhone",""+p++);
					homePhone = Cell.cell("homePhone", ""+p++);
					address1 = Cell.cell("address1", l+" No Street");
					address2 = Cell.cell("address2", "None");
					city = Cell.cell("city", "Jacksonville");
					state = Cell.cell("state", "FL");
					zip = Cell.cell("zip", ""+z++);
					
					writerReader.add(""+l++,firstName,lastName, email,mobilePhone,homePhone,address1,address2,city,state,zip);
					// Operation<Boolean> addOp = asyncAccess.add(""+l++,firstName,lastName, email,mobilePhone,homePhone,address1,address2,city,state,zip);
					 
					
					if(z % 20000 == 0){
						long end_time = System.nanoTime();
						double difference = (end_time - start_time) / 1e6;
						System.out.println( l + " reccord inserted in  " +difference + "milli seconds");
					}

				}
				System.out.println(new java.util.Date());
				System.out.println("Reccord inserted");
			}catch(Exception e2){
				e2.printStackTrace();
			}
		}catch(Exception e1){
			e1.printStackTrace();

		}

	}

}
