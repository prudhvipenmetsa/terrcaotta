package com.gcs.tcclient.customer;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.manager.DatasetManager;
import com.terracotta.store.tql.ResultStream;
import com.terracotta.store.tql.TqlEnvironment;
import com.terracottatech.store.Cell;

public class GetCustomerTCTQL {

	
		
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
					 
					 
					 final DatasetReader<String> reader = customers.reader(); // 1
					 TqlEnvironment env = new TqlEnvironment(reader, // 2
					 "customers", // 3
					 CellDefinition.defineString("firstName"),
					 CellDefinition.defineString("lastName"), 
					 CellDefinition.defineString("email"),
					 CellDefinition.defineString("mobilePhone"),
					 CellDefinition.defineString("homePhone"),
					 CellDefinition.defineString("address1"),
					 CellDefinition.defineString("address2"),
					 CellDefinition.defineString("city"),
					 CellDefinition.defineString("state"),
					 CellDefinition.defineString("zip"));
					 try (ResultStream resultStream =
							 
					 env.query("SELECT * FROM customers").stream()) {
						 resultStream.forEach(System.out::println); // 6
					 }
				 }catch(Exception e2){
					 System.out.println(e2.getMessage());
					 e2.printStackTrace();
				 }
				}catch(Exception e1){
					System.out.println(e1.getMessage());
					e1.printStackTrace();
					
				}

	}

	}

