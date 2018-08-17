package com.gcs.tcclient.customer;

import com.terracottatech.store.Cell;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.CharCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;



public class CustomerBean implements java.io.Serializable {
	public static  IntCellDefinition customerNo;
	
	public static StringCellDefinition firstName;
	public  static StringCellDefinition lastName;
	public  StringCellDefinition email;
	public  StringCellDefinition mobilePhone;
	public  StringCellDefinition homePhone;
	public  StringCellDefinition address1;
	public  StringCellDefinition address2;
	public  StringCellDefinition city;
	public  StringCellDefinition state;
	public  StringCellDefinition zip;
	
	public class FirstName implements StringCellDefinition {

		@Override
		public String name() {
			// TODO Auto-generated method stub
			return "FirstName";
		}

		@Override
		public Cell<String> newCell(String arg0) {
			// TODO Auto-generated method stub
			return Cell.cell("FirstName", arg0);
		}

		@Override
		public Type<String> type() {
			// TODO Auto-generated method stub
			return Type.STRING;
		}

	}

	

}
