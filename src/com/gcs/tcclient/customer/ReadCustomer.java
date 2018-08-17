package com.gcs.tcclient.customer;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.Cell;




public class ReadCustomer {
	
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
				 long l = 1,p=1111111111;
				 DatasetWriterReader<String> writerReader = customers.writerReader();
				 Cell<String> firstName = Cell.cell("firstName", "Prudhvi");
				 Cell<String> lastName = Cell.cell("lastName", "Penmetsa");
				 Cell<String> email = Cell.cell("email", "Prudhvi@gmail.com");
				 Cell<String> mobilePhone = Cell.cell("mobilePhone", "1234567890");
				 Cell<String> homePhone = Cell.cell("homePhone", "1234567890");
				 Cell<String> address1 = Cell.cell("address1", "1 No Street");
				 Cell<String> address2 = Cell.cell("address2", "None");
				 Cell<String> city = Cell.cell("city", "Jacksonville");
				 Cell<String> state = Cell.cell("state", "FL");
				 Cell<String> zip = Cell.cell("zip", "12346");
				 java.lang.Long ll = java.lang.Long.parseLong("1");
							 
				 //writerReader.add("p1",firstName,lastName, email);
				
				 for(int i=0;i<5000000;i++){
					 Cell<String> firstName = Cell.cell("firstName", "Prudhvi"+i);
					 Cell<String> lastName = Cell.cell("lastName", "Penmetsa"+i);
					 Cell<String> email = Cell.cell("email", "Prudhvi"+i+"@gmail.com");
					 Cell<String> mobilePhone = Cell.cell("mobilePhone",""+p++);
					 Cell<String> homePhone = Cell.cell("homePhone", ""+p++);
					 Cell<String> address1 = Cell.cell("address1", l+" No Street");
					 Cell<String> address2 = Cell.cell("address2", "None");
					 Cell<String> city = Cell.cell("city", "Jacksonville");
					 Cell<String> state = Cell.cell("state", "FL");
					 Cell<String> zip = Cell.cell("zip", ""+z++);
					 if(z>99999) z=10000;
					
								 
					 writerReader.add(""+l,firstName,lastName, email,mobilePhone,homePhone,address1,address2,city,state,zip);
					 }
				 System.out.println(p1.get("firstName").get());
				/* 
				 List<String> result = recordStream
						 .explain(System.out::println)
						 .filter(TAXONOMIC_CLASS.value().is("mammal")) // 1
						 M
						 Even Header
						 Usage and Best Practices
						 TCStore API Developer Guide Version 10.2 30
						 .map(TAXONOMIC_CLASS.valueOrFail())
						 .collect(toList())
						 */
			 }catch(Exception e2){
				 e2.printStackTrace();
			 }
			}catch(Exception e1){
				e1.printStackTrace();
				
			}

}

}
