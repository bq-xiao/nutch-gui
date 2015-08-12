package org.apache.nutch.webui.test;

import org.apache.nutch.webui.dao.HbaseBase;
import org.apache.nutch.webui.dao.impl.HbaseBaseImpl;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		HbaseBase h = new HbaseBaseImpl();
		h.crateInstanceTable();
	}

}
