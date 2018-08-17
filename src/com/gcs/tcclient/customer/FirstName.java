package com.gcs.tcclient.customer;

import com.terracottatech.store.Cell;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.StringCellDefinition;

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
