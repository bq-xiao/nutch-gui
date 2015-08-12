package org.apache.nutch.webui.dao.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.nutch.webui.dao.HbaseBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseBaseImpl implements HbaseBase {
	private static final Logger logger = LoggerFactory.getLogger(HbaseBaseImpl.class);
	private Configuration configuration = HBaseConfiguration.create();
	
	public HbaseBaseImpl(){
		logger.info("hbase host: " + configuration.get("hbase.zookeeper.quorum"));
		logger.info("hbase port: " + configuration.get("hbase.zookeeper.property.clientPort"));
	}
	
	@Override
	public void crateInstanceTable() {
		String tableName = configuration.get("instance.table.name");
		try {
			HBaseAdmin hbaseAdmin = new HBaseAdmin(configuration);
			if(!hbaseAdmin.tableExists(tableName)){
				HTableDescriptor tableDescriptor = new HTableDescriptor(tableName); 
	            tableDescriptor.addFamily(new HColumnDescriptor("instance_name")); 
	            tableDescriptor.addFamily(new HColumnDescriptor("instance_host")); 
	            tableDescriptor.addFamily(new HColumnDescriptor("instance_port"));
	            tableDescriptor.addFamily(new HColumnDescriptor("user_name")); 
	            tableDescriptor.addFamily(new HColumnDescriptor("user_password")); 
	            hbaseAdmin.createTable(tableDescriptor);
			}else{
				logger.info(tableName + " is exists.");
			}
			hbaseAdmin.close();
		} catch (MasterNotRunningException e) {
			logger.error(e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

}
